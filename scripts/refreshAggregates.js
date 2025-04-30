import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
);

const periods = [
  { name: "weekly_stats", groupBy: ["year", "week"] },
  { name: "monthly_stats", groupBy: ["year", "month"] },
  { name: "quarterly_stats", groupBy: ["year", "quarter"] },
  { name: "yearly_stats", groupBy: ["year"] },
];

async function refreshStats() {
  for (const period of periods) {
    console.log(`📊 Mise à jour des agrégats : ${period.name}`);

    const groupByCols = period.groupBy;

    const { data, error } = await supabase
      .from("ga4_snapshots")
      .select(
        "site_id, year, month, week, quarter, " +
          "sum(sessions) as sessions, " +
          "sum(pageviews) as pageviews, " +
          "sum(active_users) as active_users, " +
          "avg(avg_session_duration) as avg_duration, " +
          "avg(engagement_rate) as engagement_rate, " +
          "avg(bounce_rate) as bounce_rate"
      )
      .eq("scope", "daily")
      .group(["site_id", ...groupByCols]);

    if (error) {
      console.error(
        `❌ Erreur récupération snapshots pour ${period.name} :`,
        error.message
      );
      continue;
    }

    for (const row of data) {
      const insertData = {
        site_id: row.site_id,
        year: row.year,
        sessions: row.sessions,
        pageviews: row.pageviews,
        active_users: row.active_users,
        avg_duration: row.avg_duration,
        engagement_rate: row.engagement_rate,
        bounce_rate: row.bounce_rate,
      };

      if (row.week) insertData.week = row.week;
      if (row.month) insertData.month = row.month;
      if (row.quarter) insertData.quarter = row.quarter;

      const { error: insertError } = await supabase
        .from(period.name)
        .upsert(insertData, { onConflict: ["site_id", ...groupByCols] });

      if (insertError) {
        console.error(
          `❌ Erreur insertion dans ${period.name} :`,
          insertError.message
        );
      } else {
        console.log(`✅ ${period.name} mis à jour pour site ${row.site_id}`);
      }
    }
  }

  console.log("✅ Toutes les agrégations sont à jour");
}

// 👉 Voici la ligne qu’il te manquait :
export default refreshStats;
