import collectGa4 from "./scripts/collectGa4.js";
import refreshAggregates from "./scripts/refreshAggregates.js";

async function main() {
  console.log("🚀 Lancement collecte GA4");
  await collectGa4();

  console.log("🧠 Lancement agrégation des stats");
  await refreshAggregates();

  console.log("✅ Tous les scripts ont été exécutés avec succès");
}

main();
