package com.example.fs.io;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.example.fs.model.Profile;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Upserts slow features into the static columns of the user's partition. */
public class ProfileSink extends AsyncCqlSink<Profile> {

    private transient List<PreparedStatement> update;

    public ProfileSink(CqlConfig cql) {
        super(cql);
    }

    @Override
    protected void prepare(CqlSession session) {
        update = new ArrayList<>();
        for (String ks : cql.keyspaces) {
            update.add(session.prepare("UPDATE " + ks + ".user_agg USING TIMESTAMP ? "
                    + "SET segment = ?, kyc_level = ?, signup_date = ?, profile_ts = ? WHERE user_id = ?"));
        }
    }

    @Override
    protected List<Statement<?>> toStatements(Profile p, long nowMs) {
        LocalDate signup = p.signupDate == null ? null : LocalDate.parse(p.signupDate);
        List<Statement<?>> out = new ArrayList<>();
        for (PreparedStatement ps : update) {
            out.add(ps.bind(p.updatedAtMs * 1000, p.segment, p.kycLevel, signup,
                    Instant.ofEpochMilli(p.updatedAtMs), p.userId));
        }
        return out;
    }
}
