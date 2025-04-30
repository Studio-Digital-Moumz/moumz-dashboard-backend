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

// Fonction pour récupérer le top d'une dimension (device, country...)
async function getTopValue(propertyId, dimensionName) {
  const [res] = await analyticsDataClient.runReport({
    property: `properties/${propertyId}`,
    dateRanges: [{ startDate: "1daysAgo", endDate: "today" }],
    dimensions: [{ name: dimensionName }],
    metrics: [{ name: "sessions" }],
    limit: 1,
    orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
  });

  return res.rows?.[0]?.dimensionValues?.[0]?.value ?? null;
}

// Fonction principale
async function collect() {
  const { data: sites, error } = await supabase.from("sites").select("*");
  if (error) {
    console.error("❌ Erreur récupération sites :", error.message);
    return;
  }

  console.log(`📌 ${sites.length} site(s) à analyser`);

  for (const site of sites) {
    try {
      // Appel principal GA4
      const [res] = await analyticsDataClient.runReport({
        property: `properties/${site.ga4_property_id}`,
        dateRanges: [{ startDate: "1daysAgo", endDate: "today" }],
        metrics: [
          { name: "activeUsers" },
          { name: "sessions" },
          { name: "screenPageViews" },
          { name: "averageSessionDuration" },
          { name: "engagementRate" },
        ],
      });

      const rows = res.rows?.[0]?.metricValues ?? [];
      const activeUsers = parseInt(rows[0]?.value ?? 0);
      const sessions = parseInt(rows[1]?.value ?? 0);
      const pageviews = parseInt(rows[2]?.value ?? 0);
      const avgSessionDuration = parseFloat(rows[3]?.value ?? 0);
      const engagementRate = parseFloat(rows[4]?.value ?? 0);
      const bounceRate = 1 - engagementRate;

      // Dimensions secondaires
      const topDevice = await getTopValue(
        site.ga4_property_id,
        "deviceCategory"
      );
      const topCountry = await getTopValue(site.ga4_property_id, "country");

      // Données temporelles
      const date = new Date();
      const year = date.getUTCFullYear();
      const month = date.getUTCMonth() + 1;
      const day = date.getUTCDate();
      const quarter = Math.floor((month - 1) / 3) + 1;

      const getWeek = (d) => {
        d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
        const dayNum = d.getUTCDay() || 7;
        d.setUTCDate(d.getUTCDate() + 4 - dayNum);
        const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
        return Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
      };

      const week = getWeek(date);

      // Insertion
      const dataToInsert = {
        site_id: site.id,
        snapshot_date: new Date().toISOString().slice(0, 10),
        active_users: activeUsers,
        sessions,
        pageviews,
        avg_session_duration: avgSessionDuration,
        engagement_rate: engagementRate,
        bounce_rate: bounceRate,
        top_device: topDevice,
        top_country: topCountry,
        year,
        month,
        quarter,
        week,
        day,
      };

      console.log("📦 Données envoyées à Supabase :", dataToInsert);

      const { error: insertError } = await supabase
        .from("ga4_snapshots")
        .upsert(dataToInsert, {
          onConflict: ["site_id", "snapshot_date", "scope"],
        });

      if (insertError) {
        console.error(
          "❌ Erreur lors de l’insertion Supabase :",
          insertError.message
        );
      } else {
        console.log(`✅ Stats enregistrées pour ${site.name}`);
      }
    } catch (err) {
      console.error(`❌ Erreur GA4 pour ${site.name} :`, err.message);
    }
  }
}

collect();
