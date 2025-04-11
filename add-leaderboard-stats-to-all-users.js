const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const addLeaderboardStatsToAllUsers = async () => {
  const usersSnapshot = await db.collection("users").get();

  if (usersSnapshot.empty) {
    console.log("🚨 No users found.");
    return;
  }

  const batchLimit = 500;
  let batch = db.batch();
  let opCount = 0;

  for (const doc of usersSnapshot.docs) {
    const userRef = db.collection("users").doc(doc.id);
    const data = doc.data();

    // Only add if it doesn't exist
    if (!data.leaderboardStats) {
      batch.update(userRef, {
        leaderboardStats: {
          distance: { rank: null, percentile: null },
          rides: { rank: null, percentile: null },
          co2: { rank: null, percentile: null },
        },
      });

      opCount++;

      if (opCount === batchLimit) {
        await batch.commit();
        console.log(`✅ Committed batch of ${opCount}`);
        batch = db.batch();
        opCount = 0;
      }
    }
  }

  if (opCount > 0) {
    await batch.commit();
    console.log(`✅ Final batch committed with ${opCount} updates.`);
  }

  console.log("🎉 Done! leaderboardStats added to all users missing it.");
};

addLeaderboardStatsToAllUsers();
