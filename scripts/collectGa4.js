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

// NOUVELLE FONCTION : Collecter les performances par page
async function collectPagePerformance(propertyId, siteId) {
  try {
    const [res] = await analyticsDataClient.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [{ startDate: "1daysAgo", endDate: "today" }],
      dimensions: [{ name: "pagePath" }, { name: "pageTitle" }],
      metrics: [
        { name: "screenPageViews" },
        { name: "averageSessionDuration" },
        { name: "bounceRate" },
        { name: "entrances" },
      ],
      limit: 20, // Top 20 pages
      orderBys: [{ metric: { metricName: "screenPageViews" }, desc: true }],
    });

    if (res.rows && res.rows.length > 0) {
      console.log(`📄 Collecte de ${res.rows.length} pages pour ${siteId}`);

      for (const row of res.rows) {
        const pagePath = row.dimensionValues[0]?.value || "";
        const pageTitle = row.dimensionValues[1]?.value || "";
        const pageviews = parseInt(row.metricValues[0]?.value || 0);
        const avgTime = parseFloat(row.metricValues[1]?.value || 0);
        const bounceRate = parseFloat(row.metricValues[2]?.value || 0);
        const entrances = parseInt(row.metricValues[3]?.value || 0);

        // Skip pages système Webflow
        if (pagePath.includes("/admin") || pagePath.includes("/.well-known")) {
          continue;
        }

        const pageData = {
          site_id: siteId,
          page_path: pagePath,
          page_title: pageTitle,
          pageviews: pageviews,
          avg_time_on_page: avgTime,
          bounce_rate: bounceRate,
          entrance_rate: pageviews > 0 ? entrances / pageviews : 0,
          date: new Date().toISOString().slice(0, 10),
        };

        // Insérer dans la table page_performance
        const { error: pageError } = await supabase
          .from("page_performance")
          .upsert(pageData, {
            onConflict: ["site_id", "page_path", "date"],
          });

        if (pageError) {
          console.error("❌ Erreur page_performance :", pageError.message);
        }
      }
      console.log(`✅ Page performance collectée pour site ${siteId}`);
    } else {
      console.log(`⚠️ Aucune page trouvée pour ${siteId}`);
    }
  } catch (err) {
    console.error("❌ Erreur collectPagePerformance :", err.message);
  }
}

// NOUVELLE FONCTION : Collecter les sources de trafic
async function collectTrafficSources(propertyId, siteId) {
  try {
    const [res] = await analyticsDataClient.runReport({
      property: `properties/${propertyId}`,
      dateRanges: [{ startDate: "1daysAgo", endDate: "today" }],
      dimensions: [
        { name: "sessionDefaultChannelGrouping" },
        { name: "sessionSource" },
      ],
      metrics: [{ name: "sessions" }, { name: "newUsers" }],
      limit: 10,
      orderBys: [{ metric: { metricName: "sessions" }, desc: true }],
    });

    const trafficData = [];
    if (res.rows) {
      for (const row of res.rows) {
        trafficData.push({
          channel: row.dimensionValues[0]?.value || "Unknown",
          source: row.dimensionValues[1]?.value || "Unknown",
          sessions: parseInt(row.metricValues[0]?.value || 0),
          newUsers: parseInt(row.metricValues[1]?.value || 0),
        });
      }
    }

    return trafficData;
  } catch (err) {
    console.error("❌ Erreur collectTrafficSources :", err.message);
    return [];
  }
}

// Fonction exportée principale
export default async function collectGa4() {
  const { data: sites, error } = await supabase.from("sites").select("*");
  if (error) {
    console.error("❌ Erreur récupération sites :", error.message);
    return;
  }

  console.log(`📌 ${sites.length} site(s) à analyser`);

  for (const site of sites) {
    try {
      console.log(`🔍 Analyse du site : ${site.name}`);

      // 1. COLLECTE PRINCIPALE (votre code existant)
      const [res] = await analyticsDataClient.runReport({
        property: `properties/${site.ga4_property_id}`,
        dateRanges: [{ startDate: "1daysAgo", endDate: "today" }],
        metrics: [
          { name: "activeUsers" },
          { name: "sessions" },
          { name: "screenPageViews" },
          { name: "averageSessionDuration" },
          { name: "engagementRate" },
          { name: "newUsers" }, // NOUVELLE MÉTRIQUE
        ],
      });

      const rows = res.rows?.[0]?.metricValues ?? [];
      const activeUsers = parseInt(rows[0]?.value ?? 0);
      const sessions = parseInt(rows[1]?.value ?? 0);
      const pageviews = parseInt(rows[2]?.value ?? 0);
      const avgSessionDuration = parseFloat(rows[3]?.value ?? 0);
      const engagementRate = parseFloat(rows[4]?.value ?? 0);
      const newUsers = parseInt(rows[5]?.value ?? 0); // NOUVELLE MÉTRIQUE
      const bounceRate = 1 - engagementRate;

      // 2. DONNÉES CONTEXTUELLES
      const topDevice = await getTopValue(
        site.ga4_property_id,
        "deviceCategory"
      );
      const topCountry = await getTopValue(site.ga4_property_id, "country");
      const trafficSources = await collectTrafficSources(
        site.ga4_property_id,
        site.id
      );

      // 3. CALCULS TEMPORELS (votre code existant)
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

      // 4. DONNÉES ENRICHIES POUR IA
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
        new_users: newUsers, // NOUVEAU CHAMP à ajouter dans ga4_snapshots
        traffic_sources: JSON.stringify(trafficSources), // NOUVEAU CHAMP
        year,
        month,
        quarter,
        week,
        day,
        scope: "daily",
      };

      console.log("📦 Données principales collectées");

      // 5. INSERTION DONNÉES PRINCIPALES
      const { error: insertError } = await supabase
        .from("ga4_snapshots")
        .upsert(dataToInsert, {
          onConflict: ["site_id", "snapshot_date", "scope"],
        });

      if (insertError) {
        console.error(
          "❌ Erreur insertion ga4_snapshots :",
          insertError.message
        );
      } else {
        console.log(`✅ Stats principales enregistrées pour ${site.name}`);
      }

      // 6. NOUVELLE COLLECTE : PAGES
      if (site.ai_enabled !== false) {
        // Si l'IA est activée pour ce site
        await collectPagePerformance(site.ga4_property_id, site.id);
      }

      console.log(`🎉 Collecte complète terminée pour ${site.name}\n`);
    } catch (err) {
      console.error(`❌ Erreur GA4 pour ${site.name} :`, err.message);
    }
  }

  console.log("🏁 Collecte GA4 terminée pour tous les sites !");
}
