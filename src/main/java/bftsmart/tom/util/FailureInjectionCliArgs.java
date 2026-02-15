package bftsmart.tom.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class FailureInjectionCliArgs {

    private static final String FAILURE_SPEC_FLAG = "--failure-spec";
    private static final String FAILURE_START_UNIX_MS_FLAG = "--failure-start-unix-ms";

    private FailureInjectionCliArgs() {
    }

    public static Parsed parse(String[] args) {
        List<String> positionalArgs = new ArrayList<>();
        String failureSpecPath = null;
        Long failureStartUnixMs = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (FAILURE_SPEC_FLAG.equals(arg)) {
                if (failureSpecPath != null) {
                    throw new IllegalArgumentException("Duplicate flag: " + FAILURE_SPEC_FLAG);
                }
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + FAILURE_SPEC_FLAG);
                }
                failureSpecPath = args[++i];
                continue;
            }
            if (arg.startsWith(FAILURE_SPEC_FLAG + "=")) {
                if (failureSpecPath != null) {
                    throw new IllegalArgumentException("Duplicate flag: " + FAILURE_SPEC_FLAG);
                }
                failureSpecPath = arg.substring((FAILURE_SPEC_FLAG + "=").length());
                if (failureSpecPath.isEmpty()) {
                    throw new IllegalArgumentException("Missing value for " + FAILURE_SPEC_FLAG);
                }
                continue;
            }

            if (FAILURE_START_UNIX_MS_FLAG.equals(arg)) {
                if (failureStartUnixMs != null) {
                    throw new IllegalArgumentException("Duplicate flag: " + FAILURE_START_UNIX_MS_FLAG);
                }
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + FAILURE_START_UNIX_MS_FLAG);
                }
                failureStartUnixMs = parseUnixMs(args[++i]);
                continue;
            }
            if (arg.startsWith(FAILURE_START_UNIX_MS_FLAG + "=")) {
                if (failureStartUnixMs != null) {
                    throw new IllegalArgumentException("Duplicate flag: " + FAILURE_START_UNIX_MS_FLAG);
                }
                String startValue = arg.substring((FAILURE_START_UNIX_MS_FLAG + "=").length());
                if (startValue.isEmpty()) {
                    throw new IllegalArgumentException("Missing value for " + FAILURE_START_UNIX_MS_FLAG);
                }
                failureStartUnixMs = parseUnixMs(startValue);
                continue;
            }

            if (arg.startsWith("--failure-")) {
                throw new IllegalArgumentException("Unknown failure injection flag: " + arg);
            }

            positionalArgs.add(arg);
        }

        boolean hasSpec = failureSpecPath != null;
        boolean hasStart = failureStartUnixMs != null;
        if (hasSpec != hasStart) {
            throw new IllegalArgumentException("Both " + FAILURE_SPEC_FLAG + " and "
                    + FAILURE_START_UNIX_MS_FLAG + " must be provided together");
        }

        return new Parsed(positionalArgs, failureSpecPath, failureStartUnixMs);
    }

    private static long parseUnixMs(String value) {
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException numberFormatException) {
            throw new IllegalArgumentException("Invalid " + FAILURE_START_UNIX_MS_FLAG + " value: " + value);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException(FAILURE_START_UNIX_MS_FLAG + " must be >= 0");
        }
        return parsed;
    }

    public static final class Parsed {
        private final List<String> positionalArgs;
        private final String failureSpecPath;
        private final Long failureStartUnixMs;

        private Parsed(List<String> positionalArgs, String failureSpecPath, Long failureStartUnixMs) {
            this.positionalArgs = Collections.unmodifiableList(new ArrayList<>(positionalArgs));
            this.failureSpecPath = failureSpecPath;
            this.failureStartUnixMs = failureStartUnixMs;
        }

        public List<String> getPositionalArgs() {
            return positionalArgs;
        }

        public boolean hasFailureInjection() {
            return failureSpecPath != null && failureStartUnixMs != null;
        }

        public String getFailureSpecPath() {
            return failureSpecPath;
        }

        public long getFailureStartUnixMs() {
            if (failureStartUnixMs == null) {
                throw new IllegalStateException("Failure injection start is not set");
            }
            return failureStartUnixMs;
        }
    }
}
