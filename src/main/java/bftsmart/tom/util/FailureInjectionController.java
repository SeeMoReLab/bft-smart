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
import java.util.Arrays;
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
    private static final String ID_TOKEN_LEADER = "leader";

    private static final AtomicInteger lastLoggedPhase = new AtomicInteger(Integer.MIN_VALUE);
    private static final Object leaderWindowResolutionLock = new Object();
    private static volatile int lastResolvedLeaderWindowPhase = Integer.MIN_VALUE;
    private static volatile Map<Integer, Integer> lastResolvedLeaderWindowReplicaDelayMs = Collections.emptyMap();
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
        List<PhaseRule> phases = parsePhases(root);

        config = new Config(true, path.toAbsolutePath().toString(), startUnixMs, warmUpMs, phases);
        lastLoggedPhase.set(Integer.MIN_VALUE);
        synchronized (leaderWindowResolutionLock) {
            lastResolvedLeaderWindowPhase = Integer.MIN_VALUE;
            lastResolvedLeaderWindowReplicaDelayMs = Collections.emptyMap();
        }

        logger.info("Failure injection enabled: spec={}, startUnixMs={}, warmUpMs={}, phaseCount={}",
                config.specPath, config.startUnixMs, config.warmUpMs, config.phases.size());
    }

    public static void disable() {
        config = Config.disabled();
        lastLoggedPhase.set(Integer.MIN_VALUE);
        synchronized (leaderWindowResolutionLock) {
            lastResolvedLeaderWindowPhase = Integer.MIN_VALUE;
            lastResolvedLeaderWindowReplicaDelayMs = Collections.emptyMap();
        }
    }

    public static int getProposalDelayMs(int replicaId) {
        return getProposalDelayMs(replicaId, replicaId, null, 0);
    }

    public static void observeReplicaState(int currentLeaderId, int[] currentViewProcesses, int f) {
        Config snapshot = config;
        if (!snapshot.enabled) {
            return;
        }

        long elapsedSinceStartMs = System.currentTimeMillis() - snapshot.startUnixMs;
        long elapsedSinceWarmUpMs = elapsedSinceStartMs - snapshot.warmUpMs;
        int activePhaseIndex = findActivePhase(snapshot.phases, elapsedSinceWarmUpMs);
        logPhaseChange(snapshot, activePhaseIndex, elapsedSinceStartMs, elapsedSinceWarmUpMs);

        if (activePhaseIndex < 0) {
            return;
        }

        resolveLeaderWindowDelaysIfNeeded(snapshot, activePhaseIndex, currentLeaderId, currentViewProcesses, f);
    }

    public static int getProposalDelayMs(int replicaId, int currentLeaderId, int[] currentViewProcesses, int f) {
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

        PhaseRule phaseRule = snapshot.phases.get(activePhaseIndex);

        Integer explicitDelayMs = phaseRule.proposalDelayRule.replicaProposalDelayMs.get(replicaId);
        if (explicitDelayMs != null) {
            return explicitDelayMs;
        }

        if (phaseRule.proposalDelayRule.leaderWindowDelayMs == null) {
            return 0;
        }

        Map<Integer, Integer> leaderWindowDelays = resolveLeaderWindowDelaysIfNeeded(
                snapshot,
                activePhaseIndex,
                currentLeaderId,
                currentViewProcesses,
                f
        );
        Integer leaderWindowDelayMs = leaderWindowDelays.get(replicaId);
        return leaderWindowDelayMs == null ? 0 : leaderWindowDelayMs;
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
            ProposalDelayRule proposalDelayRule = readProposalDelayRule(phaseElement);
            phaseRules.add(new PhaseRule(startOffsetMs, proposalDelayRule, order));
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

    private static ProposalDelayRule readProposalDelayRule(Element phaseElement) {
        Element pbft = findDirectChild(phaseElement, TAG_PBFT);
        if (pbft == null) {
            return ProposalDelayRule.empty();
        }

        Element proposalDelay = findDirectChild(pbft, TAG_PROPOSAL_DELAY);
        if (proposalDelay == null) {
            return ProposalDelayRule.empty();
        }

        Integer defaultDelayMs = readIntegerTag(proposalDelay, TAG_DELAY_MS);
        Element replicas = findDirectChild(proposalDelay, TAG_REPLICAS);
        if (replicas == null) {
            return ProposalDelayRule.empty();
        }

        Map<Integer, Integer> result = new HashMap<>();
        Integer leaderWindowDelayMs = null;
        List<Element> replicaEntries = findDirectChildren(replicas, TAG_REPLICA);
        for (Element replicaEntry : replicaEntries) {
            String replicaIdText = readTagText(replicaEntry, TAG_ID);
            Integer replicaDelayMs = readIntegerTag(replicaEntry, TAG_DELAY_MS);
            if (replicaIdText == null) {
                continue;
            }
            if (replicaDelayMs == null) {
                replicaDelayMs = defaultDelayMs;
            }
            if (replicaDelayMs == null) {
                continue;
            }
            int sanitizedDelayMs = Math.max(0, replicaDelayMs);
            if (isLeaderToken(replicaIdText)) {
                leaderWindowDelayMs = sanitizedDelayMs;
                continue;
            }
            Integer replicaId = parseInteger(replicaIdText);
            if (replicaId == null) {
                continue;
            }
            result.put(replicaId, sanitizedDelayMs);
        }

        if (!replicaEntries.isEmpty()) {
            if (result.isEmpty() && leaderWindowDelayMs == null) {
                return ProposalDelayRule.empty();
            }
            return new ProposalDelayRule(Collections.unmodifiableMap(result), leaderWindowDelayMs);
        }

        if (defaultDelayMs == null) {
            return ProposalDelayRule.empty();
        }

        int sanitizedDefaultDelayMs = Math.max(0, defaultDelayMs);
        List<Element> replicaIds = findDirectChildren(replicas, TAG_ID);
        for (Element replicaIdElement : replicaIds) {
            String replicaIdText = replicaIdElement.getTextContent();
            if (replicaIdText == null) {
                continue;
            }
            replicaIdText = replicaIdText.trim();
            if (replicaIdText.isEmpty()) {
                continue;
            }
            if (isLeaderToken(replicaIdText)) {
                leaderWindowDelayMs = sanitizedDefaultDelayMs;
                continue;
            }
            Integer replicaId = parseInteger(replicaIdText);
            if (replicaId != null) {
                result.put(replicaId, sanitizedDefaultDelayMs);
            }
        }

        if (result.isEmpty() && leaderWindowDelayMs == null) {
            return ProposalDelayRule.empty();
        }
        return new ProposalDelayRule(Collections.unmodifiableMap(result), leaderWindowDelayMs);
    }

    private static boolean isLeaderToken(String replicaIdText) {
        return ID_TOKEN_LEADER.equalsIgnoreCase(replicaIdText);
    }

    private static Map<Integer, Integer> resolveLeaderWindowDelaysIfNeeded(
            Config snapshot,
            int activePhaseIndex,
            int currentLeaderId,
            int[] currentViewProcesses,
            int f
    ) {
        if (lastResolvedLeaderWindowPhase == activePhaseIndex) {
            return lastResolvedLeaderWindowReplicaDelayMs;
        }
        synchronized (leaderWindowResolutionLock) {
            if (lastResolvedLeaderWindowPhase == activePhaseIndex) {
                return lastResolvedLeaderWindowReplicaDelayMs;
            }

            Map<Integer, Integer> resolvedDelays = Collections.emptyMap();
            if (activePhaseIndex >= 0 && activePhaseIndex < snapshot.phases.size()) {
                PhaseRule phaseRule = snapshot.phases.get(activePhaseIndex);
                resolvedDelays = resolveLeaderWindowDelays(phaseRule, currentLeaderId, currentViewProcesses, f);
            }

            lastResolvedLeaderWindowReplicaDelayMs = resolvedDelays;
            lastResolvedLeaderWindowPhase = activePhaseIndex;
            return lastResolvedLeaderWindowReplicaDelayMs;
        }
    }

    private static Map<Integer, Integer> resolveLeaderWindowDelays(
            PhaseRule phaseRule,
            int currentLeaderId,
            int[] currentViewProcesses,
            int f
    ) {
        Integer delayMs = phaseRule.proposalDelayRule.leaderWindowDelayMs;
        if (delayMs == null) {
            return Collections.emptyMap();
        }
        if (currentViewProcesses == null || currentViewProcesses.length == 0) {
            logger.warn("Cannot resolve phase-start leader delay window for phase {}: current view is empty",
                    phaseRule.originalOrder);
            return Collections.emptyMap();
        }

        int[] orderedProcesses = Arrays.copyOf(currentViewProcesses, currentViewProcesses.length);
        Arrays.sort(orderedProcesses);

        int leaderPos = -1;
        for (int i = 0; i < orderedProcesses.length; i++) {
            if (orderedProcesses[i] == currentLeaderId) {
                leaderPos = i;
                break;
            }
        }
        if (leaderPos < 0) {
            logger.warn("Cannot resolve phase-start leader delay window for phase {}: leader {} not in current view {}",
                    phaseRule.originalOrder, currentLeaderId, Arrays.toString(orderedProcesses));
            return Collections.emptyMap();
        }

        int windowSize = Math.max(0, Math.min(f, orderedProcesses.length));
        if (windowSize == 0) {
            return Collections.emptyMap();
        }

        Map<Integer, Integer> resolved = new HashMap<>();
        List<Integer> targets = new ArrayList<>(windowSize);
        for (int offset = 0; offset < windowSize; offset++) {
            int replicaId = orderedProcesses[(leaderPos + offset) % orderedProcesses.length];
            resolved.put(replicaId, delayMs);
            targets.add(replicaId);
        }
        Collections.sort(targets);
        logger.info("Resolved phase-start leader proposal delay window: phase={}, leader={}, f={}, delayMs={}, targets={}",
                phaseRule.originalOrder, currentLeaderId, f, delayMs, targets);
        return Collections.unmodifiableMap(resolved);
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

    private static final class Config {
        final boolean enabled;
        final String specPath;
        final long startUnixMs;
        final long warmUpMs;
        final List<PhaseRule> phases;

        private Config(boolean enabled, String specPath, long startUnixMs, long warmUpMs,
                       List<PhaseRule> phases) {
            this.enabled = enabled;
            this.specPath = specPath;
            this.startUnixMs = startUnixMs;
            this.warmUpMs = warmUpMs;
            this.phases = phases;
        }

        static Config disabled() {
            return new Config(false, null, 0L, 0L, Collections.<PhaseRule>emptyList());
        }
    }

    private static final class PhaseRule {
        final long startOffsetMs;
        final ProposalDelayRule proposalDelayRule;
        final int originalOrder;

        private PhaseRule(long startOffsetMs, ProposalDelayRule proposalDelayRule, int originalOrder) {
            this.startOffsetMs = startOffsetMs;
            this.proposalDelayRule = proposalDelayRule;
            this.originalOrder = originalOrder;
        }
    }

    private static final class ProposalDelayRule {
        final Map<Integer, Integer> replicaProposalDelayMs;
        final Integer leaderWindowDelayMs;

        private ProposalDelayRule(Map<Integer, Integer> replicaProposalDelayMs, Integer leaderWindowDelayMs) {
            this.replicaProposalDelayMs = replicaProposalDelayMs;
            this.leaderWindowDelayMs = leaderWindowDelayMs;
        }

        static ProposalDelayRule empty() {
            return new ProposalDelayRule(Collections.<Integer, Integer>emptyMap(), null);
        }
    }
}
