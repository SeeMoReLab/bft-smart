package bftsmart.tom.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages PBFT proposal-delay fault injection from a phase-based XML spec.
 */
public final class FailureInjectionController {

    private static final Logger logger = LoggerFactory.getLogger(FailureInjectionController.class);

    private static final String TAG_PHASES = "phases";
    private static final String TAG_PHASE = "phase";
    private static final String TAG_PBFT = "pbft";
    private static final String TAG_PROPOSAL_DELAY = "proposalDelay";
    private static final String TAG_REPLICAS = "replicas";
    private static final String TAG_REPLICA = "replica";
    private static final String TAG_ID = "id";
    private static final String TAG_DELAY_MS = "delayMs";

    private static final String TAG_START_AT = "startAt";
    private static final String TAG_START_AT_MS = "startAtMs";
    private static final String TAG_AT_TIME = "atTime";
    private static final String TAG_AT_TIME_MS = "atTimeMs";
    private static final String TAG_TIME = "time";
    private static final String TAG_WARM_UP_TIME = "warmUpTime";
    private static final String TAG_WARM_UP_TIME_MS = "warmUpTimeMs";
    private static final String TAG_LEADER_FAILURE_INTERVAL = "leaderFailureInterval";
    private static final String TAG_LEADER_FAILURE_INTERVAL_MS = "leaderFailureIntervalMs";

    private static final AtomicInteger lastLoggedPhase = new AtomicInteger(Integer.MIN_VALUE);
    private static volatile Config config = Config.disabled();

    private FailureInjectionController() {
    }

    public static void configure(String specPath, long startUnixMs) {
        if (specPath == null || specPath.trim().isEmpty()) {
            throw new IllegalArgumentException("Failure injection spec path is required");
        }
        if (startUnixMs < 0) {
            throw new IllegalArgumentException("Failure injection start unix ms must be >= 0");
        }

        Path path = Paths.get(specPath).normalize();
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Failure injection spec not found: " + specPath);
        }
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Failure injection spec is not a file: " + specPath);
        }

        Document document = parseDocument(path.toFile());
        Element root = document.getDocumentElement();
        if (root == null) {
            throw new IllegalArgumentException("Invalid failure injection spec: missing root element");
        }

        long warmUpMs = readWarmUpMs(root);
        long leaderFailureIntervalMs = readLeaderFailureIntervalMs(root);
        List<PhaseRule> phases = parsePhases(root);

        config = new Config(true, path.toAbsolutePath().toString(), startUnixMs, warmUpMs, leaderFailureIntervalMs, phases);
        lastLoggedPhase.set(Integer.MIN_VALUE);

        logger.info("Failure injection enabled: spec={}, startUnixMs={}, warmUpMs={}, leaderFailureIntervalMs={}, phaseCount={}",
                config.specPath, config.startUnixMs, config.warmUpMs, config.leaderFailureIntervalMs, config.phases.size());
    }

    public static void disable() {
        config = Config.disabled();
        lastLoggedPhase.set(Integer.MIN_VALUE);
    }

    public static int getProposalDelayMs(int replicaId) {
        Config snapshot = config;
        if (!snapshot.enabled) {
            return 0;
        }

        long elapsedSinceStartMs = System.currentTimeMillis() - snapshot.startUnixMs;
        long elapsedSinceWarmUpMs = elapsedSinceStartMs - snapshot.warmUpMs;
        int activePhaseIndex = findActivePhase(snapshot.phases, elapsedSinceWarmUpMs);
        logPhaseChange(snapshot, activePhaseIndex, elapsedSinceStartMs, elapsedSinceWarmUpMs);

        if (activePhaseIndex < 0) {
            return 0;
        }

        Integer delayMs = snapshot.phases.get(activePhaseIndex).replicaProposalDelayMs.get(replicaId);
        return delayMs == null ? 0 : delayMs;
    }

    public static long getLeaderFailureIntervalMs() {
        Config snapshot = config;
        if (!snapshot.enabled) {
            return 0L;
        }
        return snapshot.leaderFailureIntervalMs;
    }

    public static long getLeaderFailureStartUnixMs() {
        Config snapshot = config;
        if (!snapshot.enabled) {
            return -1L;
        }
        return safeAdd(snapshot.startUnixMs, snapshot.warmUpMs);
    }

    public static boolean isLeaderFailureEnabled() {
        Config snapshot = config;
        return snapshot.enabled && snapshot.leaderFailureIntervalMs > 0;
    }

    private static int findActivePhase(List<PhaseRule> phases, long elapsedMs) {
        if (elapsedMs < 0 || phases.isEmpty()) {
            return -1;
        }

        int active = -1;
        for (int i = 0; i < phases.size(); i++) {
            PhaseRule phaseRule = phases.get(i);
            if (elapsedMs >= phaseRule.startOffsetMs) {
                active = i;
            } else {
                break;
            }
        }
        return active;
    }

    private static void logPhaseChange(Config snapshot, int activePhaseIndex, long elapsedSinceStartMs, long elapsedSinceWarmUpMs) {
        int previous = lastLoggedPhase.get();
        if (previous == activePhaseIndex) {
            return;
        }
        if (!lastLoggedPhase.compareAndSet(previous, activePhaseIndex)) {
            return;
        }

        if (activePhaseIndex < 0) {
            logger.info("Failure injection phase changed: inactive (elapsedSinceStartMs={}, elapsedSinceWarmUpMs={})",
                    elapsedSinceStartMs, elapsedSinceWarmUpMs);
            return;
        }

        PhaseRule phaseRule = snapshot.phases.get(activePhaseIndex);
        logger.info("Failure injection phase changed: index={}, startOffsetMs(afterWarmUp)={}, elapsedSinceStartMs={}, elapsedSinceWarmUpMs={}",
                activePhaseIndex, phaseRule.startOffsetMs, elapsedSinceStartMs, elapsedSinceWarmUpMs);
    }

    private static List<PhaseRule> parsePhases(Element root) {
        Element phasesElement = findDirectChild(root, TAG_PHASES);
        if (phasesElement == null) {
            return Collections.emptyList();
        }

        List<Element> phaseElements = findDirectChildren(phasesElement, TAG_PHASE);
        List<PhaseRule> phaseRules = new ArrayList<>(phaseElements.size());

        int order = 0;
        for (Element phaseElement : phaseElements) {
            long startOffsetMs = readPhaseStartOffsetMs(phaseElement);
            Map<Integer, Integer> replicaDelays = readReplicaDelays(phaseElement);
            phaseRules.add(new PhaseRule(startOffsetMs, replicaDelays, order));
            order++;
        }

        phaseRules.sort(Comparator
                .comparingLong((PhaseRule p) -> p.startOffsetMs)
                .thenComparingInt(p -> p.originalOrder));

        return Collections.unmodifiableList(phaseRules);
    }

    private static long readWarmUpMs(Element root) {
        Long explicitMs = readLongTag(root, TAG_WARM_UP_TIME_MS);
        if (explicitMs != null) {
            return Math.max(0L, explicitMs);
        }
        Double seconds = readDoubleTag(root, TAG_WARM_UP_TIME);
        if (seconds == null) {
            return 0L;
        }
        return Math.max(0L, Math.round(seconds * 1000.0d));
    }

    private static long readLeaderFailureIntervalMs(Element root) {
        Long explicitMs = readLongTag(root, TAG_LEADER_FAILURE_INTERVAL_MS);
        if (explicitMs != null) {
            return Math.max(0L, explicitMs);
        }
        Double seconds = readDoubleTag(root, TAG_LEADER_FAILURE_INTERVAL);
        if (seconds == null) {
            return 0L;
        }
        return Math.max(0L, Math.round(seconds * 1000.0d));
    }

    private static long readPhaseStartOffsetMs(Element phaseElement) {
        Long explicitMs = readLongTag(phaseElement, TAG_START_AT_MS);
        if (explicitMs != null) {
            return Math.max(0L, explicitMs);
        }
        explicitMs = readLongTag(phaseElement, TAG_AT_TIME_MS);
        if (explicitMs != null) {
            return Math.max(0L, explicitMs);
        }

        Double seconds = readDoubleTag(phaseElement, TAG_START_AT);
        if (seconds == null) {
            seconds = readDoubleTag(phaseElement, TAG_AT_TIME);
        }
        if (seconds == null) {
            seconds = readDoubleTag(phaseElement, TAG_TIME);
        }
        if (seconds == null) {
            return 0L;
        }

        long convertedMs = Math.round(seconds * 1000.0d);
        return Math.max(0L, convertedMs);
    }

    private static Map<Integer, Integer> readReplicaDelays(Element phaseElement) {
        Element pbft = findDirectChild(phaseElement, TAG_PBFT);
        if (pbft == null) {
            return Collections.emptyMap();
        }

        Element proposalDelay = findDirectChild(pbft, TAG_PROPOSAL_DELAY);
        if (proposalDelay == null) {
            return Collections.emptyMap();
        }

        Integer defaultDelayMs = readIntegerTag(proposalDelay, TAG_DELAY_MS);
        Element replicas = findDirectChild(proposalDelay, TAG_REPLICAS);
        if (replicas == null) {
            return Collections.emptyMap();
        }

        Map<Integer, Integer> result = new HashMap<>();
        List<Element> replicaEntries = findDirectChildren(replicas, TAG_REPLICA);
        for (Element replicaEntry : replicaEntries) {
            Integer replicaId = readIntegerTag(replicaEntry, TAG_ID);
            Integer replicaDelayMs = readIntegerTag(replicaEntry, TAG_DELAY_MS);
            if (replicaId == null) {
                continue;
            }
            if (replicaDelayMs == null) {
                replicaDelayMs = defaultDelayMs;
            }
            if (replicaDelayMs == null) {
                continue;
            }
            result.put(replicaId, Math.max(0, replicaDelayMs));
        }

        if (!replicaEntries.isEmpty()) {
            return Collections.unmodifiableMap(result);
        }

        if (defaultDelayMs == null) {
            return Collections.emptyMap();
        }

        List<Element> replicaIds = findDirectChildren(replicas, TAG_ID);
        for (Element replicaIdElement : replicaIds) {
            Integer replicaId = parseInteger(replicaIdElement.getTextContent());
            if (replicaId != null) {
                result.put(replicaId, Math.max(0, defaultDelayMs));
            }
        }

        return Collections.unmodifiableMap(result);
    }

    private static Document parseDocument(File specFile) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        } catch (ParserConfigurationException ignored) {
            // Best-effort hardening.
        }
        try {
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (ParserConfigurationException ignored) {
            // Best-effort hardening.
        }
        try {
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        } catch (ParserConfigurationException ignored) {
            // Best-effort hardening.
        }
        try {
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        } catch (ParserConfigurationException ignored) {
            // Best-effort hardening.
        }
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(specFile);
        } catch (ParserConfigurationException | SAXException | IOException exception) {
            throw new IllegalArgumentException(
                    "Failed to parse failure injection spec: " + specFile.getPath(), exception);
        }
    }

    private static Element findDirectChild(Element parent, String tagName) {
        List<Element> children = findDirectChildren(parent, tagName);
        return children.isEmpty() ? null : children.get(0);
    }

    private static List<Element> findDirectChildren(Element parent, String tagName) {
        List<Element> result = new ArrayList<>();
        NodeList childNodes = parent.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            Element element = (Element) child;
            if (tagName.equals(element.getTagName())) {
                result.add(element);
            }
        }
        return result;
    }

    private static Integer readIntegerTag(Element parent, String tagName) {
        return parseInteger(readTagText(parent, tagName));
    }

    private static Long readLongTag(Element parent, String tagName) {
        return parseLong(readTagText(parent, tagName));
    }

    private static Double readDoubleTag(Element parent, String tagName) {
        return parseDouble(readTagText(parent, tagName));
    }

    private static String readTagText(Element parent, String tagName) {
        Element child = findDirectChild(parent, tagName);
        if (child == null) {
            return null;
        }
        String content = child.getTextContent();
        if (content == null) {
            return null;
        }
        String trimmed = content.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Integer parseInteger(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Long parseLong(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Double parseDouble(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static long safeAdd(long left, long right) {
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static final class Config {
        final boolean enabled;
        final String specPath;
        final long startUnixMs;
        final long warmUpMs;
        final long leaderFailureIntervalMs;
        final List<PhaseRule> phases;

        private Config(boolean enabled, String specPath, long startUnixMs, long warmUpMs,
                       long leaderFailureIntervalMs, List<PhaseRule> phases) {
            this.enabled = enabled;
            this.specPath = specPath;
            this.startUnixMs = startUnixMs;
            this.warmUpMs = warmUpMs;
            this.leaderFailureIntervalMs = leaderFailureIntervalMs;
            this.phases = phases;
        }

        static Config disabled() {
            return new Config(false, null, 0L, 0L, 0L, Collections.<PhaseRule>emptyList());
        }
    }

    private static final class PhaseRule {
        final long startOffsetMs;
        final Map<Integer, Integer> replicaProposalDelayMs;
        final int originalOrder;

        private PhaseRule(long startOffsetMs, Map<Integer, Integer> replicaProposalDelayMs, int originalOrder) {
            this.startOffsetMs = startOffsetMs;
            this.replicaProposalDelayMs = replicaProposalDelayMs;
            this.originalOrder = originalOrder;
        }
    }
}
