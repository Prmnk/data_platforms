package com.example.fs.model;

/** Keyed record for the compacted features.changelog topic. */
public class ChangelogRecord {
    public String key;
    public String value;

    public ChangelogRecord() {}

    public ChangelogRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
