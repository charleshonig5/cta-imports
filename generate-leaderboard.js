const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const timeframes = {
  all_time: null,
  "1w": 7,
  "1m": 30,
  "1y": 365,
  ytd: "ytd",
};

const categories = ["distance", "rides", "co2"];

function isWithinTimeframe(timestamp, timeframe) {
  if (!timestamp || !timestamp.toDate) return false;
  const now = new Date();

  if (timeframe === "ytd") {
    const jan1 = new Date(now.getFullYear(), 0, 1);
    return timestamp.toDate() >= jan1;
  }

  if (typeof timeframe === "number") {
    const pastDate = new Date(now);
    pastDate.setDate(now.getDate() - timeframe);
    return timestamp.toDate() >= pastDate;
  }

  return true;
}

async function generateLeaderboard() {
  const usersSnapshot = await db.collection("users").get();
  const leaderboardData = {};

  for (const [timeframeKey, timeframeVal] of Object.entries(timeframes)) {
    const userStats = [];

    for (const userDoc of usersSnapshot.docs) {
      const userId = userDoc.id;
      const ridesSnapshot = await db.collection("users").doc(userId).collection("rides").get();

      let totalDistance = 0;
      let totalCO2 = 0;
      let rideCount = 0;

      for (const rideDoc of ridesSnapshot.docs) {
        const data = rideDoc.data();
        if (!isWithinTimeframe(data.startTime, timeframeVal)) continue;

        totalDistance += data.distance || 0;
        totalCO2 += data.co2 || 0;
        rideCount += 1;
      }

      userStats.push({
        userId,
        distance: totalDistance,
        rides: rideCount,
        co2: totalCO2,
      });
    }

    for (const category of categories) {
      const sorted = [...userStats].sort((a, b) => b[category] - a[category]).slice(0, 100);
      leaderboardData[`${category}_${timeframeKey}`] = sorted;
    }
  }

  // Upload to Firestore
  const batch = db.batch();
  for (const [docId, users] of Object.entries(leaderboardData)) {
    const docRef = db.collection("leaderboard").doc(docId);
    batch.set(docRef, { users });
  }

  await batch.commit();
  console.log("✅ Leaderboard successfully generated and uploaded!");
}

generateLeaderboard();
