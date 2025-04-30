console.log("Hello depuis Moumz GA4 🎉");
import { BetaAnalyticsDataClient } from "@google-analytics/data";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

const analyticsDataClient = new BetaAnalyticsDataClient({
  credentials: JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON),
});

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
);

async function collect() {
  const { data: sites, error } = await supabase.from("sites").select("*");
  if (error) throw error;

  for (const site of sites) {
    try {
      const [res] = await analyticsDataClient.runReport({
        property: `properties/${site.ga4_property_id}`,
        dateRanges: [{ startDate: "1daysAgo", endDate: "today" }],
        metrics: [
          { name: "activeUsers" },
          { name: "sessions" },
          { name: "screenPageViews" },
        ],
      });

      await supabase.from("ga4_snapshots").insert({
        site_id: site.id,
        snapshot_date: new Date().toISOString().slice(0, 10),
        active_users: parseInt(res.rows?.[0]?.metricValues?.[0]?.value ?? 0),
        sessions: parseInt(res.rows?.[0]?.metricValues?.[1]?.value ?? 0),
        pageviews: parseInt(res.rows?.[0]?.metricValues?.[2]?.value ?? 0),
      });

      console.log(`✅ Stats enregistrées pour ${site.name}`);
    } catch (err) {
      console.error(`❌ Erreur pour ${site.name} :`, err.message);
    }
  }
}

collect();
