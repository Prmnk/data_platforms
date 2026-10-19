package com.example.fs.model;

/** Slow-changing user attributes (in production: Debezium CDC from the users table). */
public class Profile {
    public String userId;
    public String segment;
    public String kycLevel;
    /** ISO date, e.g. 2021-03-02. */
    public String signupDate;
    public long updatedAtMs;

    public Profile() {}
}
