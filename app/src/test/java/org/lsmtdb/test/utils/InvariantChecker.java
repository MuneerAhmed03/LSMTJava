package org.lsmtdb.test.utils;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.compaction.LevelMetadata;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.TableDirectory;

import java.io.IOException;
import java.util.*;

/**
 * Helper class for verifying LSM Tree invariants.
 * Checks critical properties that must hold for correctness:
 * - Keys are sorted within each SSTable
 * - SSTables at L1+ have non-overlapping key ranges
 * - No duplicate keys within a level (L1+)
 */
public class InvariantChecker {
    
    /**
     * Represents an invariant violation found during checking.
     */
    public static class InvariantViolation {
        public final String invariantName;
        public final String description;
        public final int level;
        public final String[] affectedFiles;
        
        public InvariantViolation(String invariantName, String description, int level, String... affectedFiles) {
            this.invariantName = invariantName;
            this.description = description;
            this.level = level;
            this.affectedFiles = affectedFiles;
        }
        
        @Override
        public String toString() {
            return String.format("[L%d] %s: %s (files: %s)",
                    level, invariantName, description, 
                    affectedFiles != null ? String.join(", ", affectedFiles) : "none");
        }
    }
    
    private final TableDirectory tableDirectory;
    private final List<InvariantViolation> violations = new ArrayList<>();
    
    /**
     * Create an InvariantChecker for the default TableDirectory.
     */
    public InvariantChecker() {
        this.tableDirectory = TableDirectory.getInstance();
    }
    
    /**
     * Create an InvariantChecker for a specific TableDirectory.
     * 
     * @param tableDirectory TableDirectory to check
     */
    public InvariantChecker(TableDirectory tableDirectory) {
        if (tableDirectory == null) {
            throw new IllegalArgumentException("TableDirectory cannot be null");
        }
        this.tableDirectory = tableDirectory;
    }
    
    /**
     * Check that all SSTables have keys in sorted order.
     * 
     * @param dir TableDirectory to check
     * @return true if all SSTables are sorted, false otherwise
     */
    public boolean checkKeyOrderingInAllSSTables(TableDirectory dir) {
        boolean allOrdered = true;
        
        for (LevelMetadata level : dir.getAllLevels()) {
            for (SSTableMetadata sstable : level.getSstables()) {
                try {
                    boolean isOrdered = SSTableInspector.verifyKeyOrdering(sstable.getFilePath());
                    if (!isOrdered) {
                        violations.add(new InvariantViolation(
                                "KeyOrdering",
                                "Keys are not in sorted order",
                                level.getLevelNumber(),
                                sstable.getFilePath()
                        ));
                        allOrdered = false;
                    }
                } catch (IOException e) {
                    violations.add(new InvariantViolation(
                            "KeyOrdering",
                            "Failed to read SSTable: " + e.getMessage(),
                            level.getLevelNumber(),
                            sstable.getFilePath()
                    ));
                    allOrdered = false;
                }
            }
        }
        
        return allOrdered;
    }
    
    /**
     * Check that SSTables at a specific level have non-overlapping key ranges.
     * This invariant applies only to L1 and above (L0 can have overlaps).
     * 
     * @param level Level number to check (must be >= 1)
     * @return true if no overlaps found, false otherwise
     */
    public boolean checkNonOverlappingRanges(int level) {
        if (level < 1) {
            // L0 can have overlaps, so this invariant doesn't apply
            return true;
        }
        
        LevelMetadata levelMetadata = tableDirectory.getLevelMetadata(level);
        if (levelMetadata == null) {
            return true; // Level doesn't exist yet
        }
        
        List<SSTableMetadata> sstables = levelMetadata.getSstables();
        if (sstables.size() <= 1) {
            return true; // Can't have overlaps with 0 or 1 SSTable
        }
        
        boolean noOverlaps = true;
        
        // SSTables should be sorted by minKey, but verify it
        List<SSTableMetadata> sortedSSTables = new ArrayList<>(sstables);
        sortedSSTables.sort(Comparator.comparing(SSTableMetadata::getMinKey));
        
        for (int i = 0; i < sortedSSTables.size() - 1; i++) {
            SSTableMetadata current = sortedSSTables.get(i);
            SSTableMetadata next = sortedSSTables.get(i + 1);
            
            // Check if current.maxKey >= next.minKey (which would be an overlap)
            if (current.getMaxKey().compareTo(next.getMinKey()) >= 0) {
                violations.add(new InvariantViolation(
                        "NonOverlappingRanges",
                        String.format("SSTables have overlapping key ranges: [%s, %s] overlaps [%s, %s]",
                                keyToString(current.getMinKey()),
                                keyToString(current.getMaxKey()),
                                keyToString(next.getMinKey()),
                                keyToString(next.getMaxKey())),
                        level,
                        current.getFilePath(),
                        next.getFilePath()
                ));
                noOverlaps = false;
            }
        }
        
        return noOverlaps;
    }
    
    /**
     * Check that there are no duplicate keys within a level (L1+).
     * This is a stronger check than non-overlapping ranges.
     * 
     * @param level Level number to check (must be >= 1)
     * @return true if no duplicates found, false otherwise
     */
    public boolean checkNoIntraLevelDuplicates(int level) {
        if (level < 1) {
            // L0 can have duplicates (different versions), so this invariant doesn't apply
            return true;
        }
        
        LevelMetadata levelMetadata = tableDirectory.getLevelMetadata(level);
        if (levelMetadata == null) {
            return true; // Level doesn't exist yet
        }
        
        List<SSTableMetadata> sstables = levelMetadata.getSstables();
        if (sstables.size() <= 1) {
            return true; // Can't have duplicates with 0 or 1 SSTable
        }
        
        boolean noDuplicates = true;
        Set<ByteArrayWrapper> seenKeys = new HashSet<>();
        
        for (SSTableMetadata sstable : sstables) {
            try {
                List<byte[]> keys = SSTableInspector.readAllKeys(sstable.getFilePath());
                
                for (byte[] key : keys) {
                    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
                    
                    if (seenKeys.contains(keyWrapper)) {
                        violations.add(new InvariantViolation(
                                "NoIntraLevelDuplicates",
                                "Duplicate key found in level: " + keyToString(keyWrapper),
                                level,
                                sstable.getFilePath()
                        ));
                        noDuplicates = false;
                    }
                    
                    seenKeys.add(keyWrapper);
                }
            } catch (IOException e) {
                violations.add(new InvariantViolation(
                        "NoIntraLevelDuplicates",
                        "Failed to read SSTable: " + e.getMessage(),
                        level,
                        sstable.getFilePath()
                ));
                noDuplicates = false;
            }
        }
        
        return noDuplicates;
    }
    
    /**
     * Run all invariant checks across all levels.
     * 
     * @return true if all invariants hold, false otherwise
     */
    public boolean checkAllInvariants() {
        violations.clear();
        
        boolean allValid = true;
        
        // Check key ordering in all SSTables
        allValid &= checkKeyOrderingInAllSSTables(tableDirectory);
        
        // Check non-overlapping ranges for L1+
        for (LevelMetadata level : tableDirectory.getAllLevels()) {
            int levelNum = level.getLevelNumber();
            if (levelNum >= 1) {
                allValid &= checkNonOverlappingRanges(levelNum);
                allValid &= checkNoIntraLevelDuplicates(levelNum);
            }
        }
        
        return allValid;
    }
    
    /**
     * Get the list of invariant violations found during checks.
     * 
     * @return List of InvariantViolation objects
     */
    public List<InvariantViolation> getViolations() {
        return new ArrayList<>(violations);
    }
    
    /**
     * Print a report of all invariant violations found.
     * 
     * @return List of violation descriptions
     */
    public List<String> reportViolations() {
        List<String> report = new ArrayList<>();
        
        if (violations.isEmpty()) {
            report.add("No invariant violations found.");
            return report;
        }
        
        report.add("=== Invariant Violations Found ===");
        report.add("Total violations: " + violations.size());
        report.add("");
        
        // Group violations by invariant type
        Map<String, List<InvariantViolation>> groupedViolations = new HashMap<>();
        for (InvariantViolation v : violations) {
            groupedViolations.computeIfAbsent(v.invariantName, k -> new ArrayList<>()).add(v);
        }
        
        for (Map.Entry<String, List<InvariantViolation>> entry : groupedViolations.entrySet()) {
            report.add("--- " + entry.getKey() + " (" + entry.getValue().size() + " violations) ---");
            for (InvariantViolation v : entry.getValue()) {
                report.add("  " + v.toString());
            }
            report.add("");
        }
        
        return report;
    }
    
    /**
     * Print violations to System.out.
     */
    public void printViolations() {
        for (String line : reportViolations()) {
            System.out.println(line);
        }
    }
    
    /**
     * Clear all recorded violations.
     */
    public void clearViolations() {
        violations.clear();
    }
    
    /**
     * Convert a ByteArrayWrapper key to a readable string.
     * 
     * @param key Key to convert
     * @return String representation
     */
    private String keyToString(ByteArrayWrapper key) {
        if (key == null) {
            return "<null>";
        }
        
        try {
            return new String(key.getData(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "<binary:" + key.getData().length + " bytes>";
        }
    }
    
    /**
     * Check metadata consistency for a specific level.
     * Verifies that metadata matches actual file contents.
     * 
     * @param level Level number to check
     * @return true if metadata is consistent, false otherwise
     */
    public boolean checkMetadataConsistency(int level) {
        LevelMetadata levelMetadata = tableDirectory.getLevelMetadata(level);
        if (levelMetadata == null) {
            return true; // No metadata to check
        }
        
        boolean consistent = true;
        
        for (SSTableMetadata sstable : levelMetadata.getSstables()) {
            try {
                // Verify that min/max keys in metadata match actual file
                byte[][] actualMinMax = SSTableInspector.getMinMaxKeys(sstable.getFilePath());
                
                if (actualMinMax == null || actualMinMax.length != 2) {
                    violations.add(new InvariantViolation(
                            "MetadataConsistency",
                            "Failed to read min/max keys from SSTable",
                            level,
                            sstable.getFilePath()
                    ));
                    consistent = false;
                    continue;
                }
                
                ByteArrayWrapper actualMin = new ByteArrayWrapper(actualMinMax[0]);
                ByteArrayWrapper actualMax = new ByteArrayWrapper(actualMinMax[1]);
                
                if (!actualMin.equals(sstable.getMinKey())) {
                    violations.add(new InvariantViolation(
                            "MetadataConsistency",
                            String.format("Min key mismatch: metadata=%s, actual=%s",
                                    keyToString(sstable.getMinKey()),
                                    keyToString(actualMin)),
                            level,
                            sstable.getFilePath()
                    ));
                    consistent = false;
                }
                
                if (!actualMax.equals(sstable.getMaxKey())) {
                    violations.add(new InvariantViolation(
                            "MetadataConsistency",
                            String.format("Max key mismatch: metadata=%s, actual=%s",
                                    keyToString(sstable.getMaxKey()),
                                    keyToString(actualMax)),
                            level,
                            sstable.getFilePath()
                    ));
                    consistent = false;
                }
            } catch (IOException e) {
                violations.add(new InvariantViolation(
                        "MetadataConsistency",
                        "Failed to verify metadata: " + e.getMessage(),
                        level,
                        sstable.getFilePath()
                ));
                consistent = false;
            }
        }
        
        return consistent;
    }
}
