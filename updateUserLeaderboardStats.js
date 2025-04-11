const admin = require('firebase-admin');
const serviceAccount = require('./serviceAccountKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const timeframes = ['all_time', '1w', '1m', '1y', 'ytd'];
const categories = ['distance', 'rides', 'co2'];

async function updateUserStats() {
  const usersSnapshot = await db.collection('users').get();
  const users = usersSnapshot.docs.map(doc => ({ id: doc.id }));

  for (const timeframe of timeframes) {
    for (const category of categories) {
      const leaderboardRef = db
        .collection('leaderboards')
        .doc(timeframe)
        .collection(category);

      const leaderboardSnapshot = await leaderboardRef.orderBy('value', 'desc').get();
      const rankedUsers = leaderboardSnapshot.docs.map((doc, index) => ({
        userId: doc.id,
        rank: index + 1,
        value: doc.data().value,
      }));

      const totalUsers = rankedUsers.length;

      for (const { userId, rank } of rankedUsers) {
        const percentile = Math.round(((totalUsers - rank) / totalUsers) * 100);

        const statField = `leaderboardStats.${timeframe}.${category}`;
        const updateData = {};
        updateData[statField] = { rank, percentile };

        await db.collection('users').doc(userId).set(updateData, { merge: true });
      }
    }
  }

  console.log('✅ User leaderboard stats updated!');
}

updateUserStats().catch(console.error);
