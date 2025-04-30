import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

console.log("🔁 Démarrage du script refreshAggregates.js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
);

const periodConfigs = [
  { name: "weekly_stats", key: "week" },
  { name: "monthly_stats", key: "month" },
  { name: "quarterly_stats", key: "quarter" },
  { name: "yearly_stats", key: null }, // year only
];

function groupDataBy(records, key) {
  const groups = {};
  for (const row of records) {
    const periodKey = key
      ? `${row.site_id}_${row.year}_${row[key]}`
      : `${row.site_id}_${row.year}`;
    if (!groups[periodKey]) {
      groups[periodKey] = {
        site_id: row.site_id,
        year: row.year,
        [key]: row[key],
        sessions: 0,
        pageviews: 0,
        active_users: 0,
        avg_session_duration: [],
        engagement_rate: [],
        bounce_rate: [],
      };
    }
    const g = groups[periodKey];
    g.sessions += row.sessions;
    g.pageviews += row.pageviews;
    g.active_users += row.active_users;
    g.avg_session_duration.push(row.avg_session_duration);
    g.engagement_rate.push(row.engagement_rate);
    g.bounce_rate.push(row.bounce_rate);
  }

  return Object.values(groups).map((g) => ({
    site_id: g.site_id,
    year: g.year,
    ...(key ? { [key]: g[key] } : {}),
    sessions: g.sessions,
    pageviews: g.pageviews,
    active_users: g.active_users,
    avg_duration: avg(g.avg_session_duration),
    engagement_rate: avg(g.engagement_rate),
    bounce_rate: avg(g.bounce_rate),
  }));
}

function avg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

export default async function refreshAggregates() {
  console.log("🧠 Lancement des agrégations Supabase...");

  const { data, error } = await supabase
    .from("ga4_snapshots")
    .select("*")
    .eq("scope", "daily");

  if (error) {
    console.error("❌ Erreur récupération snapshots :", error.message);
    return;
  }

  for (const { name, key } of periodConfigs) {
    console.log(`📊 Traitement : ${name}`);
    const aggregates = groupDataBy(data, key);

    for (const row of aggregates) {
      const { error: insertError } = await supabase.from(name).upsert(row, {
        onConflict: ["site_id", "year", ...(key ? [key] : [])],
      });

      if (insertError) {
        console.error(`❌ Erreur insertion ${name} :`, insertError.message);
      } else {
        console.log(`✅ ${name} mis à jour pour ${row.site_id}`);
      }
    }
  }

  console.log("✅ Toutes les agrégations sont à jour !");
}
