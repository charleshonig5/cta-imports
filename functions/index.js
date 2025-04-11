const functions = require('firebase-functions/v2');
const admin = require('firebase-admin');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onDocumentWritten } = require('firebase-functions/v2/firestore');
const { onCall } = require("firebase-functions/v2/https");
const { FieldValue } = require('firebase-admin/firestore');

admin.initializeApp();
const db = admin.firestore();

// ---------------- LEADERBOARD ---------------- //

const TIME_PERIODS = ['all_time', '1w', '1m', '1y', 'ytd'];
const CATEGORIES = ['rides', 'distance', 'co2'];

exports.scheduledLeaderboardUpdate = onSchedule('every 15 minutes', async () => {
  for (const timePeriod of TIME_PERIODS) {
    for (const category of CATEGORIES) {
      const usersSnapshot = await db.collection('users').get();
      const users = [];

      usersSnapshot.forEach((doc) => {
        const data = doc.data();
        const key = `${timePeriod}_${category}`;
        if (data.metrics?.[key] != null) {
          users.push({ userId: doc.id, metricValue: data.metrics[key] });
        }
      });

      users.sort((a, b) => b.metricValue - a.metricValue);

      const leaderboardDocs = [];
      const totalUsers = users.length;
      let currentRank = 1;
      let prevValue = null;
      let skip = 0;

      for (let i = 0; i < users.length; i++) {
        const user = users[i];

        if (user.metricValue === prevValue) {
          skip++;
        } else {
          currentRank = i + 1 + skip;
          skip = 0;
        }

        const percentile = totalUsers === 1 ? 100 : ((totalUsers - i - 1) / (totalUsers - 1)) * 100;

        await db
          .collection('users')
          .doc(user.userId)
          .collection('leaderboardStats')
          .doc(`${timePeriod}_${category}`)
          .set({
            rank: currentRank,
            percentile: Math.round(percentile * 100) / 100,
            metricValue: user.metricValue,
            category,
            timePeriod,
          });

        if (i < 100) {
          leaderboardDocs.push({
            userId: user.userId,
            rank: currentRank,
            metricValue: user.metricValue,
          });
        }

        prevValue = user.metricValue;
      }

      await db
        .collection('leaderboards')
        .doc(`${timePeriod}_${category}`)
        .set({ top100: leaderboardDocs });
    }
  }

  console.log('🏆 Leaderboards and personal ranks updated.');
});

// ---------------- STATS + STREAKS ---------------- //

const transitTypes = ['all', 'bus', 'train'];
const timePeriods = ['allTime', '1w', '1m', '1y', 'ytd'];

exports.onRideWrite = onDocumentWritten('rides/{rideId}', async (event) => {
  const rideSnap = event.data?.after;
  if (!rideSnap) return;

  const userId = rideSnap.get('userId');
  if (!userId) return;

  const isManual = rideSnap.get('manualEntry');
  const startTime = rideSnap.get('startTime');

  if (!isManual && startTime) {
    await handleStreakUpdate(userId, startTime);
  }

  await updateRecentSelections(userId, rideSnap);

  for (const timePeriod of timePeriods) {
    for (const transitType of transitTypes) {
      const stats = await calculateStats(userId, timePeriod, transitType);

      await db
        .collection('users')
        .doc(userId)
        .collection('stats')
        .doc(`${timePeriod}_${transitType}`)
        .set({
          ...stats,
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp(),
        });
    }
  }

  console.log(`✅ Stats updated in real-time for user: ${userId}`);
});

// ---------------- HELPER: RIDE STREAK ---------------- //

async function handleStreakUpdate(userId, startTime) {
  const userRef = db.collection('users').doc(userId);
  const userSnap = await userRef.get();
  const user = userSnap.data() || {};

  const rideDate = new Date(startTime.toDate ? startTime.toDate() : startTime);
  rideDate.setHours(0, 0, 0, 0);

  const lastRideDate = user.lastRideDate?.toDate
    ? user.lastRideDate.toDate()
    : user.lastRideDate;

  let currentStreak = 1;
  let longestStreak = user.longestStreak || 0;

  if (lastRideDate) {
    const last = new Date(lastRideDate);
    last.setHours(0, 0, 0, 0);
    const diffDays = (rideDate - last) / (1000 * 60 * 60 * 24);

    if (diffDays === 1) {
      currentStreak = (user.currentStreak || 0) + 1;
    } else if (diffDays === 0) {
      currentStreak = user.currentStreak || 1;
    } else {
      currentStreak = 1;
    }
  }

  if (currentStreak > longestStreak) longestStreak = currentStreak;

  await userRef.update({
    currentStreak,
    longestStreak,
    lastRideDate: rideDate,
  });

  console.log(`🔥 Streak updated for ${userId}: ${currentStreak} day(s)`);
}

// ---------------- HELPER: RECENT SELECTIONS ---------------- //

async function updateRecentSelections(userId, rideSnap) {
  const fields = ['line', 'startStop', 'endStop'];

  for (const field of fields) {
    const value = rideSnap.get(field);
    if (!value) continue;

    const ref = db
      .collection('users')
      .doc(userId)
      .collection('recentSelections')
      .doc(field);

    const doc = await ref.get();
    const current = doc.exists ? doc.data().items || [] : [];

    const updated = [value, ...current.filter((v) => v !== value)].slice(0, 5);

    await ref.set({ items: updated });
  }
}

// ---------------- HELPER: CALCULATE STATS ---------------- //

async function calculateStats(userId, timePeriod, transitType) {
  let ridesQuery = db.collection('rides').where('userId', '==', userId);
  const now = new Date();
  let startDate;

  switch (timePeriod) {
    case '1w': startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
    case '1m': startDate = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate()); break;
    case '1y': startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()); break;
    case 'ytd': startDate = new Date(now.getFullYear(), 0, 1); break;
    default: startDate = null;
  }

  if (startDate) {
    ridesQuery = ridesQuery.where('startTime', '>=', startDate);
  }

  const ridesSnapshot = await ridesQuery.get();
  const rides = ridesSnapshot.docs
    .map((doc) => doc.data())
    .filter((ride) => transitType === 'all' || ride.type === transitType);

  let totalDistance = 0;
  let totalTime = 0;
  let totalRides = rides.length;
  let totalCost = 0;
  let co2Saved = 0;
  const lineCounts = {};
  let lastChargeTime = null;
  let longestRide = null;

  for (const ride of rides) {
    const { distanceKm, durationMinutes, startTime, type, line, startStop, endStop } = ride;

    totalDistance += distanceKm;
    totalTime += durationMinutes;

    const rideStart = new Date(startTime.toDate ? startTime.toDate() : startTime);
    const cost = type === 'bus' ? 2.25 : 2.5;

    if (!lastChargeTime || rideStart.getTime() - lastChargeTime.getTime() > 2 * 60 * 60 * 1000) {
      totalCost += cost;
      lastChargeTime = rideStart;
    }

    if (type === 'bus') co2Saved += distanceKm * 0.15;
    if (type === 'train') co2Saved += distanceKm * 0.2;

    if (!longestRide || distanceKm > longestRide.distanceKm) {
      longestRide = { distanceKm, line, startStop, endStop };
    }

    if (line) lineCounts[line] = (lineCounts[line] || 0) + 1;
  }

  const mostUsedLineEntry = Object.entries(lineCounts).sort((a, b) => b[1] - a[1])[0];
  const mostUsedLine = mostUsedLineEntry?.[0] || null;
  const mostUsedLineCount = mostUsedLineEntry?.[1] || null;

  const costPerMile = totalDistance > 0 ? totalCost / totalDistance : 0;
  const totalTimeHours = Math.floor(totalTime / 60);
  const totalTimeRemainingMinutes = totalTime % 60;

  let co2Change = null;
  let averageDistancePerWeek = 0;

  if (startDate) {
    const durationWeeks = (now - startDate) / (7 * 24 * 60 * 60 * 1000);
    if (durationWeeks > 0) averageDistancePerWeek = totalDistance / durationWeeks;
  }

  if (timePeriod === 'allTime') {
    const startOfThisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    const startOfLastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);

    const thisMonth = await db.collection('rides')
      .where('userId', '==', userId)
      .where('startTime', '>=', startOfThisMonth)
      .get();

    const lastMonth = await db.collection('rides')
      .where('userId', '==', userId)
      .where('startTime', '>=', startOfLastMonth)
      .where('startTime', '<', startOfThisMonth)
      .get();

    const co2This = thisMonth.docs
      .map((doc) => doc.data())
      .filter((ride) => transitType === 'all' || ride.type === transitType)
      .reduce((sum, ride) => sum + ride.distanceKm * (ride.type === 'bus' ? 0.15 : 0.2), 0);

    const co2Last = lastMonth.docs
      .map((doc) => doc.data())
      .filter((ride) => transitType === 'all' || ride.type === transitType)
      .reduce((sum, ride) => sum + ride.distanceKm * (ride.type === 'bus' ? 0.15 : 0.2), 0);

    co2Change = co2This - co2Last;
  }

  const longestRideMiles = longestRide ? longestRide.distanceKm * 0.621371 : 0;
  const longestRideLine = longestRide?.line || null;
  const longestRideRoute = longestRide?.startStop && longestRide?.endStop
    ? `${longestRide.startStop} → ${longestRide.endStop}`
    : null;

  return {
    totalDistance,
    totalTimeMinutes: totalTime,
    totalTimeHours,
    totalTimeRemainingMinutes,
    totalRides,
    totalCost,
    co2Saved,
    mostUsedLine,
    mostUsedLineCount,
    costPerMile,
    averageDistancePerWeek,
    co2Change,
    longestRideMiles,
    longestRideLine,
    longestRideRoute,
  };
}

// ---------------- PUSH NOTIFICATIONS ---------------- //

exports.sendRideReminder = onCall(async (request) => {
  const { userId } = request.data;
  if (!userId) throw new Error("Missing userId");

  const userDoc = await db.collection("users").doc(userId).get();
  const user = userDoc.data();

  if (!user?.fcmToken) {
    throw new Error("No FCM token found for user.");
  }

  const payload = {
    notification: {
      title: "Don't forget to start tracking!",
      body: "It looks like you may be on a ride. Tap to start tracking your stats.",
    },
  };

  await admin.messaging().sendToDevice(user.fcmToken, payload);
  console.log(`✅ Notification sent to ${userId}`);
  return { success: true };
});
