const functions = require('firebase-functions/v2');
const admin = require('firebase-admin');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onDocumentCreated, onDocumentUpdated, onDocumentWritten, onDocumentDeleted } = require('firebase-functions/v2/firestore');
const { onCall } = require("firebase-functions/v2/https");
const { onObjectFinalized } = require('firebase-functions/v2/storage');
const { FieldValue } = require('firebase-admin/firestore');
const sharp = require('sharp'); // Required for profile photo compression


admin.initializeApp();
const db = admin.firestore();
const storage = admin.storage();

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

/**
 * 🔄 Update User Stats on Ride Write (Create or Update)
 */
exports.onRideWrite = onDocumentWritten('rides/{rideId}', async (event) => {
  const rideSnap = event.data?.after;
  if (!rideSnap) return;

  const userId = rideSnap.get('userId');
  if (!userId) return;

  const isManual = rideSnap.get('manualEntry');
  const inProgress = rideSnap.get('inProgress') || false;
  const startTime = rideSnap.get('startTime');

  if (inProgress) {
    console.log(`🚧 Ride in progress, skipping stats update for now: ${userId}`);
    return;
  }

  if (!isManual && startTime) {
    await handleStreakUpdate(userId, startTime);
  }

  await updateRecentSelections(userId, rideSnap);
  await updateDetailStats(userId); // ✅ Added here

  // Loop through all time periods and transit types and update stats
  for (const timePeriod of timePeriods) {
    for (const transitType of transitTypes) {
      const stats = await calculateStats(userId, timePeriod, transitType);

      await db
        .collection('users')
        .doc(userId)
        .collection('stats')
        .doc(`${timePeriod}_${transitType}`)
        .set({
          totalDistance: stats.totalDistance,
          averageDistancePerWeek: stats.averageDistancePerWeek,
          totalTimeMinutes: stats.totalTimeMinutes,
          totalTimeHours: stats.totalTimeHours,
          totalTimeRemainingMinutes: stats.totalTimeRemainingMinutes,
          totalRides: stats.totalRides,
          rideCountChange: stats.rideCountChange || 0, // 🚀 New Stat Added Here
          totalCost: stats.totalCost,
          costPerMile: stats.costPerMile,
          co2Saved: stats.co2Saved,
          co2Change: stats.co2Change || 0, // 🚀 New Stat Added Here
          mostUsedLine: stats.mostUsedLine,
          mostUsedLineCount: stats.mostUsedLineCount,
          longestRideMiles: stats.longestRideMiles,
          longestRideLine: stats.longestRideLine,
          longestRideRoute: stats.longestRideRoute,
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp(),
        });
    }
  }

  console.log(`✅ Stats updated in real-time for user: ${userId}`);
});

/**
 * 🗑️ Cleanup and Update User Stats on Ride Delete
 */
exports.onRideDelete = onDocumentDeleted('rides/{rideId}', async (event) => {
  const deletedRide = event.data?.data();
  if (!deletedRide) return;

  const userId = deletedRide.userId;
  if (!userId) return;

  const inProgress = deletedRide.inProgress || false;

  if (inProgress) {
    console.log(`🗑️ In-progress ride deleted, skipping stats cleanup: ${userId}`);
    return;
  }

  // Loop through all time periods and transit types and update stats after deletion
  for (const timePeriod of timePeriods) {
    for (const transitType of transitTypes) {
      const stats = await calculateStats(userId, timePeriod, transitType);

      await db
        .collection('users')
        .doc(userId)
        .collection('stats')
        .doc(`${timePeriod}_${transitType}`)
        .set({
          totalDistance: stats.totalDistance,
          averageDistancePerWeek: stats.averageDistancePerWeek,
          totalTimeMinutes: stats.totalTimeMinutes,
          totalTimeHours: stats.totalTimeHours,
          totalTimeRemainingMinutes: stats.totalTimeRemainingMinutes,
          totalRides: stats.totalRides,
          rideCountChange: stats.rideCountChange || 0, // 🚀 Monthly change patched here
          totalCost: stats.totalCost,
          costPerMile: stats.costPerMile,
          co2Saved: stats.co2Saved,
          co2Change: stats.co2Change || 0, // 🚀 Monthly change patched here
          mostUsedLine: stats.mostUsedLine,
          mostUsedLineCount: stats.mostUsedLineCount,
          longestRideMiles: stats.longestRideMiles,
          longestRideLine: stats.longestRideLine,
          longestRideRoute: stats.longestRideRoute,
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp(),
        });
    }
  }

  await updateRecentSelections(userId, null);
  await updateDetailStats(userId); // ✅ Added here

  console.log(`🗑️ Ride deleted and stats cleanup completed for user: ${userId}`);
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
  if (!rideSnap) return;

  const fields = ['line', 'startStop', 'endStop'];

  for (const field of fields) {
    const value = rideSnap.get(field);
    const type = rideSnap.get('type');
    const line = rideSnap.get('line');

    if (!value || !type) continue;

    const docId = `${type}_${line || 'none'}_${field}`;

    const ref = db
      .collection('users')
      .doc(userId)
      .collection('recentSearches')
      .doc(docId);

    const doc = await ref.get();
    const current = doc.exists ? doc.data().items || [] : [];

    const updated = [value, ...current.filter((v) => v !== value)].slice(0, 5);

    await ref.set({ items: updated });
  }

  const startStop = rideSnap.get('startStop');
  if (startStop) {
    const ref = db
      .collection('users')
      .doc(userId)
      .collection('fallbackSearches')
      .doc('startStop');

    const doc = await ref.get();
    const current = doc.exists ? doc.data().items || [] : [];

    const updated = [startStop, ...current.filter((v) => v !== startStop)].slice(0, 5);

    await ref.set({ items: updated });
  }
}
// ---------------- UPDATED CALCULATE STATS ---------------- //

async function calculateStats(userId, timePeriod, transitType) {
  let ridesQuery = db.collection('rides').where('userId', '==', userId);
  const now = new Date();
  let startDate;

  // Set startDate based on time period
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

  // Fetch and filter rides
  const ridesSnapshot = await ridesQuery.get();
  const rides = ridesSnapshot.docs
    .map((doc) => doc.data())
    .filter((ride) => 
      (transitType === 'all' || ride.type === transitType) && 
      !ride.inProgress 
    );

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

    co2Saved += distanceKm * (type === 'bus' ? 0.15 : 0.2);

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

  // Calculate the average distance per week
  let averageDistancePerWeek = 0;
  if (startDate) {
    const durationWeeks = (now - startDate) / (7 * 24 * 60 * 60 * 1000);
    if (durationWeeks > 0) averageDistancePerWeek = totalDistance / durationWeeks;
  }

  // Monthly change calculations
  let rideCountChange = null;
  let co2Change = null;

  if (timePeriod === 'allTime') {
    const startOfThisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    const startOfLastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);

    // Ride Count Change
    const ridesLastMonthSnapshot = await db.collection('rides')
      .where('userId', '==', userId)
      .where('startTime', '>=', startOfLastMonth)
      .where('startTime', '<', startOfThisMonth)
      .get();

    const ridesLastMonth = ridesLastMonthSnapshot.docs
      .map((doc) => doc.data())
      .filter((ride) => 
        (transitType === 'all' || ride.type === transitType) &&
        !ride.inProgress
      ).length;

    rideCountChange = totalRides - ridesLastMonth;

    // CO₂ Saved Change
    const co2LastMonth = ridesLastMonthSnapshot.docs
      .map((doc) => doc.data())
      .reduce((sum, ride) => 
        sum + ride.distanceKm * (ride.type === 'bus' ? 0.15 : 0.2), 0
      );

    co2Change = co2Saved - co2LastMonth;
  }

  const longestRideMiles = longestRide ? longestRide.distanceKm * 0.621371 : 0;
  const longestRideLine = longestRide?.line || null;
  const longestRideRoute = longestRide?.startStop && longestRide?.endStop
    ? `${longestRide.startStop} → ${longestRide.endStop}`
    : null;

  return {
    totalDistance,
    averageDistancePerWeek,
    totalTimeMinutes: totalTime,
    totalTimeHours,
    totalTimeRemainingMinutes,
    totalRides,
    rideCountChange,
    totalCost,
    costPerMile,
    co2Saved,
    co2Change,
    mostUsedLine,
    mostUsedLineCount,
    longestRideMiles,
    longestRideLine,
    longestRideRoute,
  };
}

// ---------------- HELPER: DETAILED STATS ---------------- //

async function updateDetailStats(userId) {
  for (const timePeriod of timePeriods) {
    for (const transitType of transitTypes) {
      const details = await calculateDetailStats(userId, timePeriod, transitType);
      await db
        .collection('users')
        .doc(userId)
        .collection('detailStats')
        .doc(`${timePeriod}_${transitType}`)
        .set({
          ...details,
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp(),
        });
    }
  }
}

async function calculateDetailStats(userId, timePeriod, transitType) {
  let ridesQuery = db.collection('rides').where('userId', '==', userId);
  const now = new Date();
  let startDate;

  switch (timePeriod) {
    case '1w': startDate = new Date(now.getTime() - 7 * 86400000); break;
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
    .map(doc => doc.data())
    .filter(ride => !ride.inProgress && (transitType === 'all' || ride.type === transitType));

  const lineStats = {};
  const stopVisits = {};
  const longestRides = [];
  let totalCost = 0;
  let lastChargeTime = null;

  for (const ride of rides) {
    const {
      line, distanceKm = 0, durationMinutes = 0, startStop, endStop,
      startTime, type, rideId, stopCount = 0
    } = ride;

    const rideStart = new Date(startTime?.toDate ? startTime.toDate() : startTime);
    const cost = type === 'bus' ? 2.25 : 2.5;

    if (!lastChargeTime || rideStart - lastChargeTime > 2 * 60 * 60 * 1000) {
      totalCost += cost;
      lastChargeTime = rideStart;
    }

    if (!lineStats[line]) {
      lineStats[line] = {
        totalDistanceKm: 0,
        totalMinutes: 0,
        rideCount: 0,
        co2Kg: 0
      };
    }

    lineStats[line].totalDistanceKm += distanceKm;
    lineStats[line].totalMinutes += durationMinutes;
    lineStats[line].rideCount += 1;
    lineStats[line].co2Kg += distanceKm * (type === 'bus' ? 0.15 : 0.2);

    if (!stopVisits[line]) stopVisits[line] = {};
    if (startStop) stopVisits[line][startStop] = (stopVisits[line][startStop] || 0) + 1;
    if (endStop) stopVisits[line][endStop] = (stopVisits[line][endStop] || 0) + 1;

    longestRides.push({
      rideId,
      line,
      distanceKm,
      startStop,
      endStop,
      stopCount
    });
  }

  const topByDistance = Object.entries(lineStats)
    .sort((a, b) => b[1].totalDistanceKm - a[1].totalDistanceKm)
    .slice(0, 5)
    .map(([line, data]) => ({ line, ...data }));

  const topByTime = Object.entries(lineStats)
    .sort((a, b) => b[1].totalMinutes - a[1].totalMinutes)
    .slice(0, 5)
    .map(([line, data]) => ({ line, totalMinutes: data.totalMinutes }));

  const topByRides = Object.entries(lineStats)
    .sort((a, b) => b[1].rideCount - a[1].rideCount)
    .slice(0, 5)
    .map(([line, data]) => ({ line, rideCount: data.rideCount }));

  const topByCO2 = Object.entries(lineStats)
    .sort((a, b) => b[1].co2Kg - a[1].co2Kg)
    .slice(0, 5)
    .map(([line, data]) => ({ line, co2Kg: data.co2Kg }));

  const costPerLine = Object.entries(lineStats)
    .map(([line, data]) => ({
      line,
      costPerMile: data.totalDistanceKm > 0 ? totalCost / (data.totalDistanceKm * 0.621371) : 0
    }))
    .sort((a, b) => b.costPerMile - a.costPerMile)
    .slice(0, 5);

  const mostUsedLineEntry = Object.entries(lineStats)
    .sort((a, b) => b[1].rideCount - a[1].rideCount)[0];

  const mostUsedLine = mostUsedLineEntry?.[0];
  const mostUsedLineDetails = mostUsedLine ? {
    line: mostUsedLine,
    longestRideStops: Math.max(...longestRides.filter(r => r.line === mostUsedLine).map(r => r.stopCount || 0)),
    topStops: Object.entries(stopVisits[mostUsedLine] || {})
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([stop]) => stop)
  } : null;

  const longestRideList = longestRides
    .sort((a, b) => b.distanceKm - a.distanceKm)
    .slice(0, 5);

  return {
    distanceTopLines: topByDistance,
    timeTopLines: topByTime,
    rideTopLines: topByRides,
    co2TopLines: topByCO2,
    costAnalysis: {
      totalSavings: totalCost,
      topExpensiveRoutes: costPerLine
    },
    mostUsedLineDetails,
    longestRides: longestRideList
  };
}

// ---------------- PUSH NOTIFICATIONS ---------------- //

// Send notification suggesting the user to start a ride
exports.sendRideStartReminder = onCall(async (request) => {
  const { userId } = request.data;
  if (!userId) throw new Error("Missing userId");

  const userDoc = await db.collection("users").doc(userId).get();
  const user = userDoc.data();

  if (!user?.fcmToken) {
    throw new Error("No FCM token found for user.");
  }

  const payload = {
    notification: {
      title: "Ready to Ride?",
      body: "It looks like you're near transit. Tap to start tracking your ride!",
    },
  };

  await admin.messaging().sendToDevice(user.fcmToken, payload);
  console.log(`✅ Start ride notification sent to ${userId}`);
  return { success: true };
});

// Send notification suggesting the user to end a ride
exports.sendRideEndReminder = onCall(async (request) => {
  const { userId } = request.data;
  if (!userId) throw new Error("Missing userId");

  const userDoc = await db.collection("users").doc(userId).get();
  const user = userDoc.data();

  if (!user?.fcmToken) {
    throw new Error("No FCM token found for user.");
  }

  const payload = {
    notification: {
      title: "End your Ride?",
      body: "It looks like you might have finished your ride. Tap to end tracking!",
    },
  };

  await admin.messaging().sendToDevice(user.fcmToken, payload);
  console.log(`✅ End ride notification sent to ${userId}`);
  return { success: true };
});

// Scheduled function to smartly detect users needing ride reminders
exports.smartRideNotificationSweep = onSchedule("every 5 minutes", async (event) => {
  console.log("🚀 Running smart ride notification sweep...");

  const snapshot = await db.collection("users").get();
  const now = Date.now();

  for (const userDoc of snapshot.docs) {
    const userData = userDoc.data();
    const userId = userDoc.id;

    if (!userData?.fcmToken) continue; // Skip if no device token

    const lastRideStart = userData.lastRideStartTime?.toMillis?.() || 0;
    const lastMotion = userData.lastMotionTimestamp?.toMillis?.() || 0;
    const isRiding = userData.isCurrentlyRiding || false;

    const hoursSinceLastRide = (now - lastRideStart) / (1000 * 60 * 60);
    const minutesSinceLastMotion = (now - lastMotion) / (1000 * 60);

    let sentNotification = false;

    // Logic: Remind user to start a ride
    if (!isRiding && hoursSinceLastRide > 4) {
      console.log(`🚲 Sending Start Ride reminder to ${userId}`);
      await admin.messaging().sendToDevice(userData.fcmToken, {
        notification: {
          title: "Ready to Ride?",
          body: "It looks like it's been a while. Start tracking your next ride!",
        },
      });
      sentNotification = true;
    }

    // Logic: Remind user to end a ride
    if (isRiding && minutesSinceLastMotion > 10) {
      console.log(`🏁 Sending End Ride reminder to ${userId}`);
      await admin.messaging().sendToDevice(userData.fcmToken, {
        notification: {
          title: "End your Ride?",
          body: "It looks like you've been stopped for a while. End your ride if finished!",
        },
      });
      sentNotification = true;
    }

    if (!sentNotification) {
      console.log(`ℹ️ No notifications needed for ${userId} this cycle.`);
    }
  }

  console.log("✅ Smart ride notification sweep complete.");
});


// ---------------- ESTIMATE RIDE TIME & DISTANCE ---------------- //

exports.estimateRideTimeAndDistance = onCall(async (request) => {
  const { routeId, directionId, startStopId, endStopId } = request.data;

  if (!routeId || !directionId || !startStopId || !endStopId) {
    throw new Error("Missing required parameters.");
  }

  const tripsSnapshot = await db.collection('trips')
    .where('route_id', '==', routeId)
    .where('direction_id', '==', directionId)
    .limit(1)
    .get();

  if (tripsSnapshot.empty) {
    throw new Error("No matching trip found.");
  }

  const tripId = tripsSnapshot.docs[0].id;

  const startSnap = await db.doc(`stop_times/${tripId}/stops/${startStopId}`).get();
  const endSnap = await db.doc(`stop_times/${tripId}/stops/${endStopId}`).get();

  if (!startSnap.exists || !endSnap.exists) {
    throw new Error("Start or end stop not found in this trip.");
  }

  const start = startSnap.data();
  const end = endSnap.data();

  const durationSeconds = end.arrivalTimeSeconds - start.arrivalTimeSeconds;
  const distanceKm = end.shapeDistTraveled - start.shapeDistTraveled;

  if (durationSeconds <= 0 || distanceKm < 0) {
    throw new Error("Invalid stop order or data.");
  }

  return {
    durationSeconds,
    distanceKm: Math.round(distanceKm * 1000) / 1000
  };
});
// ---------------- LIVE RIDE TRACKING FUNCTIONS ---------------- //

// Start a new live ride
exports.startLiveRide = onCall(async (request) => {
  const { userId, type, line, startStop } = request.data;
  if (!userId || !type || !startStop) {
    throw new Error('Missing required parameters.');
  }

  const rideData = {
    userId,
    startTime: FieldValue.serverTimestamp(),
    type,
    line: line || null,
    startStop,
    endStop: null,
    inProgress: true,
    distanceMiles: 0,
    durationSeconds: 0,
    manualEntry: false,
  };

  const rideRef = await db.collection('rides').add(rideData);

  console.log(`🚀 Live ride started for user: ${userId}, rideId: ${rideRef.id}`);
  return { rideId: rideRef.id };
});

// Update an active live ride
exports.updateLiveRide = onCall(async (request) => {
  const { rideId, distanceIncrementMiles, timeIncrementSeconds } = request.data;

  if (!rideId || distanceIncrementMiles == null || timeIncrementSeconds == null) {
    throw new Error('Missing required parameters.');
  }

  const rideRef = db.collection('rides').doc(rideId);
  const rideSnap = await rideRef.get();

  if (!rideSnap.exists) {
    throw new Error('Ride not found.');
  }

  const ride = rideSnap.data();

  if (!ride.inProgress) {
    throw new Error('Cannot update a completed ride.');
  }

  await rideRef.update({
    distanceMiles: FieldValue.increment(distanceIncrementMiles),
    durationSeconds: FieldValue.increment(timeIncrementSeconds),
  });

  console.log(`⏱️ Ride updated: ${rideId} (+${distanceIncrementMiles} mi, +${timeIncrementSeconds}s)`);
  return { success: true };
});

// End a live ride (finalize)
exports.endLiveRide = onCall(async (request) => {
  const { rideId, endStop } = request.data;

  if (!rideId || !endStop) {
    throw new Error('Missing required parameters.');
  }

  const rideRef = db.collection('rides').doc(rideId);
  const rideSnap = await rideRef.get();

  if (!rideSnap.exists) {
    throw new Error('Ride not found.');
  }

  const ride = rideSnap.data();

  if (!ride.inProgress) {
    throw new Error('Ride already completed.');
  }

  const distanceMiles = ride.distanceMiles || 0;
  const durationMinutes = Math.round((ride.durationSeconds || 0) / 60);
  const distanceKm = distanceMiles * 1.60934;

  await rideRef.update({
    inProgress: false,
    endStop,
    distanceKm,
    durationMinutes,
    updatedAt: FieldValue.serverTimestamp(),
  });

  console.log(`✅ Ride finalized for rideId: ${rideId}`);
  return { success: true };
});

// Discard a live ride (delete)
exports.discardLiveRide = onCall(async (request) => {
  const { rideId } = request.data;

  if (!rideId) {
    throw new Error('Missing rideId.');
  }

  await db.collection('rides').doc(rideId).delete();

  console.log(`🗑️ Live ride discarded: ${rideId}`);
  return { success: true };
});
// ---------------- OPTIMIZE PROFILE PHOTO ON UPLOAD ---------------- //
// This Cloud Function triggers when a new profile photo is uploaded to Storage.
// It auto-compresses and resizes the image to 512x512 JPG format to save bandwidth and storage.

exports.optimizeProfilePhoto = onObjectFinalized({
  region: 'us-central1',
  eventFilters: [
    { attribute: 'bucket', value: 'transit-stats.appspot.com' },
    { attribute: 'name', value: 'profilePhotos/**' }
  ]
}, async (event) => {
  const filePath = event.data.name;
  const bucket = storage.bucket(event.data.bucket);
  const tempFilePath = `/tmp/original.jpg`;
  const outputPath = `/tmp/optimized.jpg`;

  // 🔒 Only process files inside the "profilePhotos/" folder
  if (!filePath || !filePath.startsWith('profilePhotos/')) return;

  const file = bucket.file(filePath);
  await file.download({ destination: tempFilePath });

  await sharp(tempFilePath)
    .resize(512, 512)
    .jpeg({ quality: 70 })
    .toFile(outputPath);

  await bucket.upload(outputPath, {
    destination: filePath,
    metadata: { contentType: 'image/jpeg' },
  });

  console.log(`✅ Compressed and optimized profile photo: ${filePath}`);
});

// ----- PRO UPGRADE FUNCTIONS ----- //

// Callable function to upgrade a user to Pro
exports.upgradeUserToPro = onCall(async (request) => {
  const uid = request.auth?.uid;

  if (!uid) {
    throw new functions.https.HttpsError('unauthenticated', 'User must be authenticated.');
  }

  const userRef = db.collection('users').doc(uid);

  await userRef.update({
    isPro: true
  });

  return { success: true, message: 'User upgraded to Pro.' };
});

// (Optional) Callable function to revoke Pro status
exports.revokeProStatus = onCall(async (request) => {
  const uid = request.auth?.uid;

  if (!uid) {
    throw new functions.https.HttpsError('unauthenticated', 'User must be authenticated.');
  }

  const userRef = db.collection('users').doc(uid);

  await userRef.update({
    isPro: false
  });

  return { success: true, message: 'Pro status revoked.' };
});

// ---------------- ACHIEVEMENTS ---------------- //

const unlockAchievement = async (userId, achievementId) => {
  const userRef = db.collection('users').doc(userId);
  const achievementRef = userRef.collection('achievements').doc(achievementId);
  const alreadyUnlocked = (await achievementRef.get()).exists;

  if (!alreadyUnlocked) {
    const globalRef = db.collection('achievements').doc(achievementId);
    const globalDoc = await globalRef.get();
    if (!globalDoc.exists) return;

    const { name, description, category } = globalDoc.data();

    await achievementRef.set({
      unlocked: true,
      unlockedAt: FieldValue.serverTimestamp(),
      name,
      description,
      category
    });

    await userRef.collection('uiState').doc('achievementPopup').set({
      achievementId,
      name,
      description,
      category,
      imageUrl,
      shown: false,
      unlockedAt: FieldValue.serverTimestamp()
    });
  }
};

exports.onRideCreated = onDocumentCreated("users/{userId}/rides/{rideId}", async (event) => {
  const userId = event.params.userId;
  const ride = event.data?.data();

  if (!ride) return;

  const userRef = db.collection('users').doc(userId);
  const snapshot = await userRef.collection('rides').get();
  const totalRides = snapshot.size;

  const userDoc = await userRef.get();
  const userData = userDoc.data();
  const totalDistance = userData?.stats?.totalDistance || 0;
  const totalCO2 = userData?.stats?.totalCO2Saved || 0;

  // RIDE COUNT achievements
  if (totalRides === 1) await unlockAchievement(userId, "getting_started");
  if (totalRides === 10) await unlockAchievement(userId, "getting_the_hang");
  if (totalRides === 25) await unlockAchievement(userId, "city_commuter");
  if (totalRides === 50) await unlockAchievement(userId, "transit_regular");
  if (totalRides === 100) await unlockAchievement(userId, "transit_hero");
  if (totalRides === 250) await unlockAchievement(userId, "ultimate_rider");

  // DISTANCE achievements
  if (totalDistance >= 10) await unlockAchievement(userId, "warming_up");
  if (totalDistance >= 25) await unlockAchievement(userId, "rolling_along");
  if (totalDistance >= 50) await unlockAchievement(userId, "transit_star");
  if (totalDistance >= 100) await unlockAchievement(userId, "transit_veteran");
  if (totalDistance >= 250) await unlockAchievement(userId, "master_of_the_map");

  // CO2 achievements
  if (totalCO2 >= 10) await unlockAchievement(userId, "carbon_kicker");
  if (totalCO2 >= 25) await unlockAchievement(userId, "eco_rider");
  if (totalCO2 >= 50) await unlockAchievement(userId, "planet_mover");
  if (totalCO2 >= 100) await unlockAchievement(userId, "green_machine");
  if (totalCO2 >= 250) await unlockAchievement(userId, "sustainability_hero");

  // SPECIALTY ride-based achievements
  const isLive = ride.wasLiveTracked;
  const stopCount = ride?.stopCount || 0;
  const startStop = ride?.startStopId;
  const endStop = ride?.endStopId;
  const timestamp = ride?.timestamp?.toDate?.() || new Date(ride?.timestamp?._seconds * 1000 || Date.now());
  const hour = timestamp.getHours();

  if (isLive && hour >= 23) await unlockAchievement(userId, "night_owl");
  if (isLive && hour < 6) await unlockAchievement(userId, "early_bird");

  if (startStop && endStop && startStop === endStop) {
    await unlockAchievement(userId, "loop_de_loop");
  }

  if (stopCount === 1) await unlockAchievement(userId, "one_stop_wonder");
  if (stopCount >= 15) await unlockAchievement(userId, "scenic_route");

  // LINE COMPLETION ACHIEVEMENTS --------------------
  const lineId = ride?.lineId;
  const type = ride?.type;

  if (lineId && type) {
    const linesUsedRef = db.collection("users").doc(userId).collection("linesUsed").doc("lines");
    const linesUsedDoc = await linesUsedRef.get();
    let trainLines = linesUsedDoc.exists ? linesUsedDoc.data().trainLines || [] : [];
    let busLines = linesUsedDoc.exists ? linesUsedDoc.data().busLines || [] : [];

    const isNewTrainLine = type === "train" && !trainLines.includes(lineId);
    const isNewBusLine = type === "bus" && !busLines.includes(lineId);

    if (isNewTrainLine) {
      trainLines.push(lineId);
      await linesUsedRef.set({ trainLines }, { merge: true });

      const allTrainLines = ["Red", "Blue", "Brown", "Green", "Orange", "Purple", "Pink", "Yellow"];
      const allUsed = allTrainLines.every(l => trainLines.includes(l));
      if (allUsed) {
        await unlockAchievement(userId, "all_aboard");
      }
    }

    if (isNewBusLine) {
      busLines.push(lineId);
      await linesUsedRef.set({ busLines }, { merge: true });

      if (busLines.length >= 120) {
        await unlockAchievement(userId, "wheels_of_the_city");
      }
    }
  }
});

exports.onUserUpdated = onDocumentUpdated("users/{userId}", async (event) => {
  const before = event.data?.before?.data();
  const after = event.data?.after?.data();
  const userId = event.params.userId;

  if (!before || !after) return;

  const wasPro = before.isPro || false;
  const isNowPro = after.isPro || false;

  if (!wasPro && isNowPro) {
    await unlockAchievement(userId, "pro_status");
  }
});

// ---------------- RIDE STREAK ACHIEVEMENTS ---------------- //

exports.onStreakUpdate = onDocumentWritten("users/{userId}/streaks", async (event) => {
  const userId = event.params.userId;
  const after = event.data?.after?.data();

  if (!after) return;

  const currentStreak = after.currentStreak || 0;

  // Ride streak milestone achievements
  if (currentStreak === 3) await unlockAchievement(userId, "quick_streak");
  if (currentStreak === 7) await unlockAchievement(userId, "one_week_warrior");
  if (currentStreak === 14) await unlockAchievement(userId, "on_a_roll");
  if (currentStreak === 30) await unlockAchievement(userId, "cta_loyalist");
  if (currentStreak === 60) await unlockAchievement(userId, "unstoppable");
});

// ---------------- SHARE ACHIEVEMENT ---------------- //

exports.recordShareAction = onCall(async (request) => {
  const uid = request.auth?.uid;
  if (!uid) {
    throw new functions.https.HttpsError('unauthenticated', 'User must be authenticated.');
  }

  await unlockAchievement(uid, "sharing_is_caring");

  console.log(`🔗 Share action recorded and achievement unlocked for ${uid}`);
  return { success: true };
});
