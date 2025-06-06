const functions = require('firebase-functions/v2');
const admin = require('firebase-admin');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onDocumentCreated, onDocumentUpdated, onDocumentWritten, onDocumentDeleted } = require('firebase-functions/v2/firestore');
const { onCall } = require("firebase-functions/v2/https");
const { onObjectFinalized } = require('firebase-functions/v2/storage');
const { FieldValue } = require('firebase-admin/firestore');
const sharp = require('sharp'); // Required for profile photo compression
const leoProfanity = require("leo-profanity");
const customBannedUsernames = ['admin', 'moderator', 'support', 'cta', 'transitstats', 'fuck'];
const pendingUpdates = new Map();
const MAX_PENDING_SIZE = 1000; // Safety limit to prevent memory leaks



admin.initializeApp();
const db = admin.firestore();
const storage = admin.storage();

// ---------------- SAFE NUMBER HELPERS ---------------- //
function safeNumber(value, defaultValue = 0) {
  const num = parseFloat(value);
  return !isNaN(num) && isFinite(num) ? num : defaultValue;
}

function safeDivide(numerator, denominator, defaultValue = 0) {
  if (!denominator || denominator === 0) return defaultValue;
  const result = numerator / denominator;
  return !isNaN(result) && isFinite(result) ? result : defaultValue;
}

/// ---------------- RIDE VALIDATION ---------------- //

function validateRide(ride) {
  if (!ride) {
    return { valid: false, reason: 'No ride data' };
  }

  // Skip in-progress rides - they're incomplete by design
  if (ride.inProgress) {
    return { valid: true };
  }

  // Check required fields for completed rides
  const requiredFields = ['startTime', 'type', 'startStop', 'endStop'];
  for (const field of requiredFields) {
    if (!ride[field]) {
      return { valid: false, reason: `Missing required field: ${field}` };
    }
  }

  // Validate transit type
  if (!['bus', 'train'].includes(ride.type)) {
    return { valid: false, reason: `Invalid transit type: ${ride.type}` };
  }

  // Validate distances (you store both distanceMiles and distanceKm)
  if (ride.distanceKm !== undefined && (typeof ride.distanceKm !== 'number' || ride.distanceKm < 0 || ride.distanceKm > 500)) {
    return { valid: false, reason: `Invalid distanceKm: ${ride.distanceKm}` };
  }

  if (ride.distanceMiles !== undefined && (typeof ride.distanceMiles !== 'number' || ride.distanceMiles < 0 || ride.distanceMiles > 300)) {
    return { valid: false, reason: `Invalid distanceMiles: ${ride.distanceMiles}` };
  }

  // Validate duration
  if (ride.durationMinutes !== undefined && (typeof ride.durationMinutes !== 'number' || ride.durationMinutes < 0 || ride.durationMinutes > 300)) {
    return { valid: false, reason: `Invalid durationMinutes: ${ride.durationMinutes}` };
  }

  if (ride.durationSeconds !== undefined && (typeof ride.durationSeconds !== 'number' || ride.durationSeconds < 0 || ride.durationSeconds > 18000)) {
    return { valid: false, reason: `Invalid durationSeconds: ${ride.durationSeconds}` };
  }

  return { valid: true };
}

// ---------------- LEADERBOARD ---------------- //

const TIME_PERIODS = ['all_time', '1w', '1m', '1y', 'ytd'];
const CATEGORIES = ['rides', 'distance', 'co2'];

// Process leaderboard for a specific time period and category
async function processLeaderboardCategory(timePeriod, category) {
  const metricField = `metrics.${timePeriod}_${category}`;
  
  // 🔥 NEW: Define "active" based on the time period (SAFE VERSION)
  let activeUserCutoff;
  switch(timePeriod) {
    case '1w':
      activeUserCutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000); // 30 days
      break;
    case '1m':
      activeUserCutoff = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000); // 90 days
      break;
    case '1y':
    case 'ytd':
      activeUserCutoff = new Date(Date.now() - 400 * 24 * 60 * 60 * 1000); // 13 months
      break;
    case 'all_time':
      activeUserCutoff = new Date(Date.now() - 730 * 24 * 60 * 60 * 1000); // 2 years
      break;
    default:
      activeUserCutoff = new Date(Date.now() - 180 * 24 * 60 * 60 * 1000); // 6 months fallback
  }
  
  // 🔥 NEW: Only get recently active users
  const usersQuery = await db.collection('users')
    .where('lastRideDate', '>', activeUserCutoff)
    .where(metricField, '>', 0)
    .orderBy('lastRideDate', 'desc') // Must order by first where field
    .orderBy(metricField, 'desc')     // Then by metric
    .select('metrics', 'lastRideDate') // Only fetch needed fields
    .get();

  // 🔥 NEW: Log how many users we're processing
  console.log(`📊 Processing ${usersQuery.size} active users for ${category} leaderboard (${timePeriod})`);

  if (usersQuery.empty) {
    console.log(`ℹ️ No active users found for ${category} leaderboard (${timePeriod})`);
    return;
  }

  // Step 2: Process ranks and update users in batches (same logic as before)
  const leaderboardDocs = [];
  let batch = db.batch();
  let batchCount = 0;
  
  let currentRank = 1;
  let prevValue = null;
  let skip = 0;

  for (let i = 0; i < usersQuery.docs.length; i++) {
    const userDoc = usersQuery.docs[i];
    const userId = userDoc.id;
    const metricValue = userDoc.data().metrics[`${timePeriod}_${category}`];

    // Calculate rank (handle ties) - exact same logic as before
    if (metricValue === prevValue) {
      skip++;
    } else {
      currentRank = i + 1;
      skip = 0;
    }

    // Calculate percentile - exact same logic as before
    const percentile = usersQuery.docs.length === 1 ? 100 : ((usersQuery.docs.length - i - 1) / (usersQuery.docs.length - 1)) * 100;

    // Update user's personal leaderboard stats - exact same structure
    const userStatsRef = db
      .collection('users')
      .doc(userId)
      .collection('leaderboardStats')
      .doc(`${timePeriod}_${category}`);

    batch.set(userStatsRef, {
      rank: currentRank,
      percentile: Math.round(percentile * 100) / 100,
      metricValue: metricValue,
      category,
      timePeriod,
    });

    // Collect top 100 for global leaderboard - exact same logic
    if (i < 100) {
      leaderboardDocs.push({
        userId: userId,
        rank: currentRank,
        metricValue: metricValue,
      });
    }

    batchCount++;
    prevValue = metricValue;

    // Commit batch every 500 operations (Firestore limit) - same as before
    if (batchCount >= 500) {
      await batch.commit();
      batch = db.batch();
      batchCount = 0;
      // Add small delay between batches
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  // Commit any remaining operations
  if (batchCount > 0) {
    await batch.commit();
  }

  // Step 3: Update global leaderboard (top 100) - exact same structure
  await db
    .collection('leaderboards')
    .doc(`${timePeriod}_${category}`)
    .set({ 
      top100: leaderboardDocs,
      updatedAt: FieldValue.serverTimestamp(),
      totalUsers: usersQuery.docs.length 
    });

  console.log(`✅ Updated ${category} leaderboard for ${timePeriod}: ${usersQuery.docs.length} users processed`);
}

// Update frequently-viewed leaderboards hourly
exports.scheduledLeaderboardUpdateHourly = onSchedule('every 1 hours', async () => {
  try {
    // Only update weekly and monthly leaderboards (users check these most)
    const quickPeriods = ['1w', '1m'];
    
    for (const timePeriod of quickPeriods) {
      for (const category of CATEGORIES) {
        await processLeaderboardCategory(timePeriod, category);
      }
    }
    
    console.log('🏆 Hourly leaderboards updated (1w, 1m).');
  } catch (error) {
    console.error('❌ Error updating hourly leaderboards:', error);
  }
});

// Update long-term leaderboards daily
exports.scheduledLeaderboardUpdateDaily = onSchedule('every 24 hours', async () => {
  try {
    // Update yearly and all-time leaderboards (less frequently checked)
    const slowPeriods = ['1y', 'all_time', 'ytd'];
    
    for (const timePeriod of slowPeriods) {
      for (const category of CATEGORIES) {
        await processLeaderboardCategory(timePeriod, category);
      }
    }
    
    console.log('🏆 Daily leaderboards updated (1y, all_time, ytd).');
  } catch (error) {
    console.error('❌ Error updating daily leaderboards:', error);
  }
});

// ---------------- NIGHTLY STATS ACCURACY CHECK ---------------- //

exports.nightlyStatsAccuracyCheck = onSchedule('every 24 hours', async () => {
  console.log('🌙 Starting nightly stats accuracy verification...');
  
  const usersSnapshot = await db.collection('users')
    .where('lastRideDate', '>', thirtyDaysAgo())
    .get();
  
  let checked = 0;
  let corrected = 0;
  
  for (const userDoc of usersSnapshot.docs) {
    try {
      // Get one sample stat to check if recalculation is needed
      const sampleStatRef = db.collection('users').doc(userDoc.id)
        .collection('stats').doc('allTime_all');
      const sampleStat = await sampleStatRef.get();
      
      if (sampleStat.exists) {
        const lastUpdated = sampleStat.data().updatedAt?.toMillis() || 0;
        const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
        
        // Only recalculate if stats are more than a day old
        if (lastUpdated < oneDayAgo) {
          await updateAllStatsAndDetailsEfficiently(userDoc.id);
          corrected++;
        }
      }
      
      checked++;
      
      // Pace ourselves to avoid overwhelming Firestore
      if (checked % 100 === 0) {
        console.log(`📊 Progress: Checked ${checked} users, corrected ${corrected}`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    } catch (error) {
      console.error(`Error checking stats for user ${userDoc.id}:`, error);
    }
  }
  
  console.log(`✅ Verified stats accuracy for ${checked} active users, corrected ${corrected}`);
});

// ---------------- STATS + STREAKS ---------------- //

const transitTypes = ['all', 'bus', 'train'];
const timePeriods = ['allTime', '1w', '1m', '1y', 'ytd'];

/**
 * 🔄 Update User Stats on Ride Write (Create or Update) - WITH DEBOUNCING
 */
exports.onRideWrite = onDocumentWritten('users/{userId}/rides/{rideId}', async (event) => {
  try {
    const rideSnap = event.data?.after;
    if (!rideSnap || !rideSnap.exists) return;

    const rideData = rideSnap.data();
    const userId = event.params.userId; // ✅ FIXED: Use params
    const rideId = event.params.rideId;

    // 🔥 NEW: Validate ride data
    const validation = validateRide(rideData);
    if (!validation.valid) {
      console.error(`❌ Invalid ride ${rideId} for user ${userId}: ${validation.reason}`);
      return; // Skip processing invalid rides
    }

    const isManual = rideData.manualEntry;
    const inProgress = rideData.inProgress || false;
    const startTime = rideData.startTime;

    if (inProgress) {
      console.log(`🚧 Ride in progress, skipping stats update for now: ${userId}`);
      return;
    }

   // IMMEDIATE UPDATES (no debouncing needed)
    if (!isManual && startTime) {
      await handleStreakUpdate(userId, startTime);
    }
    await updateRecentSelections(userId, rideSnap);
    
    // 🔥 NEW: Increment stats immediately for instant feedback
    await incrementStatsForNewRide(userId, rideData);

    // Safety check - prevent unbounded growth
    if (pendingUpdates.size > MAX_PENDING_SIZE) {
      console.error(`⚠️ Clearing pendingUpdates - size exceeded ${MAX_PENDING_SIZE}`);
      for (const timeoutId of pendingUpdates.values()) {
        clearTimeout(timeoutId);
      }
      pendingUpdates.clear();
    }

    // Clear existing timeout
    if (pendingUpdates.has(userId)) {
      clearTimeout(pendingUpdates.get(userId));
      console.log(`⏳ Clearing previous stats update for ${userId}`);
    }

    // Schedule full recalculation as verification (longer delay now)
    const timeoutId = setTimeout(async () => {
      try {
        console.log(`📊 Running full stats verification for user: ${userId}`);
        
        // This now serves as accuracy check rather than primary update
        await updateAllStatsAndDetailsEfficiently(userId);
        await syncMetricsForLeaderboards(userId);
        await checkRelevantAchievements(userId, rideData);
        
      } catch (error) {
        console.error(`❌ Error in stats verification for ${userId}:`, error);
      } finally {
        pendingUpdates.delete(userId);
      }
    }, 30000); // Increased to 30 seconds since stats already updated

    pendingUpdates.set(userId, timeoutId);
    console.log(`⏱️ Stats verification scheduled for ${userId} in 30 seconds (pending: ${pendingUpdates.size})`);

  } catch (error) {
    console.error(`❌ Error in onRideWrite for user ${event.params.userId}:`, error);
  }
});

/**
 * 🗑️ Cleanup and Update User Stats on Ride Delete
 */
exports.onRideDelete = onDocumentDeleted('users/{userId}/rides/{rideId}', async (event) => {
  try {
    const deletedRide = event.data?.data();
    if (!deletedRide) return;

    const userId = event.params.userId; // ✅ FIXED: Use params
    const rideId = event.params.rideId; // ✅ Also get rideId from params

    const inProgress = deletedRide.inProgress || false;

    if (inProgress) {
      console.log(`🗑️ In-progress ride deleted, skipping stats cleanup: ${userId}`);
      return;
    }

    // 🚀 OPTIMIZED: Update ALL stats efficiently with single read after deletion
    await updateAllStatsAndDetailsEfficiently(userId);

    await updateRecentSelections(userId, null);

    // 🔥 Sync metrics field for leaderboards after deletion
    console.log(`📊 Syncing metrics field for leaderboards after deletion: ${userId}`);
    await syncMetricsForLeaderboards(userId);

    console.log(`🗑️ Ride deleted and stats cleanup completed efficiently for user: ${userId}`);
  } catch (error) {
    console.error(`❌ Error cleaning up stats after ride deletion for user ${event.params.userId}:`, error);
  }
});

// 🚀 NEW OPTIMIZED FUNCTION: Update all stats and details with single read + BATCHING
async function updateAllStatsAndDetailsEfficiently(userId) {
  try {
    console.log(`📊 Starting efficient stats update for user: ${userId}`);
    
    // Step 1: Read ALL user rides once (the only database read!)
    const allRidesSnapshot = await db.collection('users').doc(userId).collection('rides').get();
    const allRides = allRidesSnapshot.docs
      .map((doc) => doc.data())
      .filter((ride) => !ride.inProgress); // Filter out in-progress rides

    // 🔥 NEW: Create single batch for all writes
    const batch = db.batch();

    // Step 2: Process all combinations and ADD TO BATCH (don't write yet)
    for (const timePeriod of timePeriods) {
      for (const transitType of transitTypes) {
        const filteredRides = filterRidesForPeriodAndType(allRides, timePeriod, transitType);
        const stats = calculateStatsFromRides(filteredRides, userId, timePeriod, transitType);
        const detailStats = calculateDetailStatsFromRides(filteredRides, timePeriod, transitType);
        
        // Add to batch instead of writing immediately
        const statsRef = db.collection('users').doc(userId).collection('stats').doc(`${timePeriod}_${transitType}`);
        const detailRef = db.collection('users').doc(userId).collection('detailStats').doc(`${timePeriod}_${transitType}`);
        
        batch.set(statsRef, {
          totalDistance: stats.totalDistance,
          averageDistancePerWeek: stats.averageDistancePerWeek,
          totalTimeMinutes: stats.totalTimeMinutes,
          totalTimeHours: stats.totalTimeHours,
          totalTimeRemainingMinutes: stats.totalTimeRemainingMinutes,
          totalRides: stats.totalRides,
          rideCountChange: stats.rideCountChange || 0,
          totalCost: stats.totalCost,
          costPerMile: stats.costPerMile,
          co2Saved: stats.co2Saved,
          co2Change: stats.co2Change || 0,
          mostUsedLine: stats.mostUsedLine,
          mostUsedLineCount: stats.mostUsedLineCount,
          longestRideMiles: stats.longestRideMiles,
          longestRideLine: stats.longestRideLine,
          longestRideRoute: stats.longestRideRoute,
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp(),
        });
        
        batch.set(detailRef, {
          ...detailStats,
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp(),
        });
      }
    }
    
    // 🚀 SINGLE DATABASE OPERATION: Write all 30 documents at once
    await batch.commit();
    
    console.log(`✅ Efficient stats update completed for user: ${userId}`);
  } catch (error) {
    console.error(`❌ Failed to update stats for ${userId}:`, error);
    // Don't let one user's bad data break the whole function
  }
}

// Helper function: Filter rides based on time period and transit type
function filterRidesForPeriodAndType(allRides, timePeriod, transitType) {
  const now = new Date();
  let startDate;

  // Same filtering logic as your current calculateStats
  switch (timePeriod) {
    case '1w': startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
    case '1m': startDate = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate()); break;
    case '1y': startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()); break;
    case 'ytd': startDate = new Date(now.getFullYear(), 0, 1); break;
    default: startDate = null; // allTime
  }

  return allRides.filter((ride) => {
    // Filter by time period
    if (startDate) {
      const rideTime = new Date(ride.startTime?.toDate ? ride.startTime.toDate() : ride.startTime);
      if (rideTime < startDate) return false;
    }
    
    // Filter by transit type
    if (transitType !== 'all' && ride.type !== transitType) return false;
    
    return true;
  });
}

// 🔥 HELPER FUNCTION: Sync Metrics for Leaderboards (unchanged)
async function syncMetricsForLeaderboards(userId) {
  const metricsUpdate = {};

  // Map your timePeriods to leaderboard TIME_PERIODS
  const leaderboardTimePeriods = {
    'allTime': 'all_time',
    '1w': '1w', 
    '1m': '1m',
    '1y': '1y',
    'ytd': 'ytd'
  };

  // Categories that leaderboards track
  const categories = ['rides', 'distance', 'co2'];

  for (const [statsTimePeriod, leaderboardTimePeriod] of Object.entries(leaderboardTimePeriods)) {
    // Get stats for 'all' transit types (for leaderboards)
    const statsDoc = await db
      .collection('users')
      .doc(userId)
      .collection('stats')
      .doc(`${statsTimePeriod}_all`)
      .get();

    if (statsDoc.exists) {
      const stats = statsDoc.data();
      
      // Map categories to StatSummary fields
      for (const category of categories) {
        let metricValue = 0;
        
        switch(category) {
          case 'rides': 
            metricValue = stats.totalRides || 0; 
            break;
          case 'distance': 
            metricValue = stats.totalDistance || 0; 
            break;
          case 'co2': 
            metricValue = stats.co2Saved || 0; 
            break;
        }
        
        metricsUpdate[`${leaderboardTimePeriod}_${category}`] = metricValue;
      }
    }
  }

  // Update main user document with metrics for leaderboards
  if (Object.keys(metricsUpdate).length > 0) {
    await db.collection('users').doc(userId).update({
      metrics: metricsUpdate
    });
    console.log(`✅ Metrics synced for user ${userId}:`, metricsUpdate);
  }
}

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

// ---------------- OPTIMIZED CALCULATE STATS HELPER FUNCTIONS (NO DATABASE READS) ---------------- //

// Modified calculateStats to work with pre-filtered rides (no database reads!)
function calculateStatsFromRides(rides, userId, timePeriod, transitType) {
  try {
    // Sort rides by time (same as original)
    rides.sort((a, b) => {
      const timeA = new Date(a.startTime?.toDate ? a.startTime.toDate() : a.startTime);
      const timeB = new Date(b.startTime?.toDate ? b.startTime.toDate() : b.startTime);
      return timeA - timeB;
    });

    let totalDistance = 0;
    let totalTime = 0;
    let totalRides = rides.length;
    let totalCost = 0;
    let co2Saved = 0;
    const lineCounts = {};
    let lastChargeTime = null;
    let longestRide = null;

    // Same calculation logic as your original calculateStats function
    for (const ride of rides) {
      const { distanceKm, durationMinutes, startTime, type, line, startStop, endStop } = ride;

      // 🔥 NULL CHECK FIXES: Add || 0 defaults to prevent NaN
      totalDistance += (distanceKm || 0);
      totalTime += (durationMinutes || 0);

      const rideStart = new Date(startTime?.toDate ? startTime.toDate() : startTime);
      const cost = type === 'bus' ? 2.25 : 2.5;

      if (!lastChargeTime || rideStart.getTime() - lastChargeTime.getTime() > 2 * 60 * 60 * 1000) {
        totalCost += cost;
        lastChargeTime = rideStart;
      }

      // 🔥 NULL CHECK FIX: Ensure distanceKm exists before CO2 calculation
      co2Saved += (distanceKm || 0) * (type === 'bus' ? 0.15 : 0.2);

      // 🔥 NULL CHECK FIX: Ensure distanceKm exists before longest ride comparison
      if (!longestRide || (distanceKm || 0) > (longestRide.distanceKm || 0)) {
        longestRide = { distanceKm: (distanceKm || 0), line, startStop, endStop };
      }

      // 🔥 NULL CHECK FIX: Only count lines that actually exist
      if (line && line.trim()) {
        lineCounts[line] = (lineCounts[line] || 0) + 1;
      }
    }

    const mostUsedLineEntry = Object.entries(lineCounts).sort((a, b) => b[1] - a[1])[0];
    const mostUsedLine = mostUsedLineEntry?.[0] || null;
    const mostUsedLineCount = mostUsedLineEntry?.[1] || null;

    const costPerMile = safeDivide(totalCost, totalDistance, 0);
    const totalTimeHours = Math.floor(totalTime / 60);
    const totalTimeRemainingMinutes = totalTime % 60;

    const now = new Date();
    let averageDistancePerWeek = 0;
    
    // Calculate average distance per week based on time period
    if (timePeriod !== 'allTime') {
      let startDate;
      switch (timePeriod) {
        case '1w': startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); break;
        case '1m': startDate = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate()); break;
        case '1y': startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()); break;
        case 'ytd': startDate = new Date(now.getFullYear(), 0, 1); break;
      }
      if (startDate) {
        const durationWeeks = (now - startDate) / (7 * 24 * 60 * 60 * 1000);
        averageDistancePerWeek = safeDivide(totalDistance, durationWeeks, 0);
      }
    }

    // Monthly change calculations (optimized to use already-filtered rides)
    let rideCountChange = null;
    let co2Change = null;

    if (timePeriod === 'allTime') {
      const startOfThisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
      const startOfLastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);

      // Filter from already-loaded rides instead of making another database query
      const lastMonthRides = rides.filter(ride => {
        const rideTime = new Date(ride.startTime?.toDate ? ride.startTime.toDate() : ride.startTime);
        return rideTime >= startOfLastMonth && rideTime < startOfThisMonth &&
               (transitType === 'all' || ride.type === transitType);
      });

      const ridesLastMonth = lastMonthRides.length;
      rideCountChange = totalRides - ridesLastMonth;

      // 🔥 NULL CHECK FIX: Calculate CO2 from filtered rides
      const co2LastMonth = lastMonthRides
        .reduce((sum, ride) => 
          sum + (ride.distanceKm || 0) * (ride.type === 'bus' ? 0.15 : 0.2), 0
        );

      co2Change = co2Saved - co2LastMonth;
    }

    const longestRideMiles = longestRide ? (longestRide.distanceKm || 0) * 0.621371 : 0;
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
  } catch (error) {
    console.error(`Error calculating stats for ${userId}: ${error}`);
    return {
      totalDistance: 0,
      averageDistancePerWeek: 0,
      totalTimeMinutes: 0,
      totalTimeHours: 0,
      totalTimeRemainingMinutes: 0,
      totalRides: 0,
      rideCountChange: 0,
      totalCost: 0,
      costPerMile: 0,
      co2Saved: 0,
      co2Change: 0,
      mostUsedLine: null,
      mostUsedLineCount: 0,
      longestRideMiles: 0,
      longestRideLine: null,
      longestRideRoute: null,
    };
  }
}

// Modified calculateDetailStats to work with pre-filtered rides (no database reads!)
function calculateDetailStatsFromRides(rides, timePeriod, transitType) {
  try {
    // Sort rides by time (same as original)
    rides.sort((a, b) => {
      const timeA = new Date(a.startTime?.toDate ? a.startTime.toDate() : a.startTime);
      const timeB = new Date(b.startTime?.toDate ? b.startTime.toDate() : b.startTime);
      return timeA - timeB;
    });

    const lineStats = {};
    const stopVisits = {};
    const longestRides = [];
    let totalCost = 0;
    let lastChargeTime = null;
    const lineCosts = {};

    // Same detailed calculation logic as your original calculateDetailStats function
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

      // 🔥 NULL CHECK FIX: Only process rides with valid line data
      if (!line || !line.trim()) continue;

      if (!lineCosts[line]) {
        lineCosts[line] = {
          totalCost: 0,
          lastChargeTime: null
        };
      }

      if (!lineCosts[line].lastChargeTime || rideStart - lineCosts[line].lastChargeTime > 2 * 60 * 60 * 1000) {
        lineCosts[line].totalCost += cost;
        lineCosts[line].lastChargeTime = rideStart;
      }

      if (!lineStats[line]) {
        lineStats[line] = {
          totalDistanceKm: 0,
          totalMinutes: 0,
          rideCount: 0,
          co2Kg: 0
        };
      }

      // 🔥 NULL CHECK FIXES: Ensure values exist before adding
      lineStats[line].totalDistanceKm += (distanceKm || 0);
      lineStats[line].totalMinutes += (durationMinutes || 0);
      lineStats[line].rideCount += 1;
      lineStats[line].co2Kg += (distanceKm || 0) * (type === 'bus' ? 0.15 : 0.2);

      if (!stopVisits[line]) stopVisits[line] = {};
      if (startStop && startStop.trim()) {
        stopVisits[line][startStop] = (stopVisits[line][startStop] || 0) + 1;
      }
      if (endStop && endStop.trim()) {
        stopVisits[line][endStop] = (stopVisits[line][endStop] || 0) + 1;
      }

      longestRides.push({
        rideId: rideId || null,
        line,
        distanceKm: (distanceKm || 0),
        startStop,
        endStop,
        stopCount: (stopCount || 0)
      });
    }

    // Rest of the logic exactly the same as your original...
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
        costPerMile: data.totalDistanceKm > 0 ? (lineCosts[line]?.totalCost || 0) / (data.totalDistanceKm * 0.621371) : 0
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
  } catch (error) {
    console.error(`Error calculating detail stats: ${error}`);
    return {
      distanceTopLines: [],
      timeTopLines: [],
      rideTopLines: [],
      co2TopLines: [],
      costAnalysis: { totalSavings: 0, topExpensiveRoutes: [] },
      mostUsedLineDetails: null,
      longestRides: []
    };
  }
}

// ---------------- INCREMENTAL STATS UPDATE ---------------- //

async function incrementStatsForNewRide(userId, ride) {
  const batch = db.batch();
  
  // Increment stats for each time period and transit type
  for (const timePeriod of timePeriods) {
    for (const transitType of transitTypes) {
      // Skip if ride doesn't match criteria
      if (transitType !== 'all' && ride.type !== transitType) continue;
      if (!isRideInTimePeriod(ride, timePeriod)) continue;
      
      const statsRef = db.collection('users').doc(userId)
        .collection('stats').doc(`${timePeriod}_${transitType}`);
      
      // Check if stats document exists
      const statsDoc = await statsRef.get();
      if (!statsDoc.exists) {
        // Initialize with this ride's data if first ride
        batch.set(statsRef, {
          totalDistance: ride.distanceKm || 0,
          totalRides: 1,
          totalTimeMinutes: ride.durationMinutes || 0,
          totalTimeHours: Math.floor((ride.durationMinutes || 0) / 60),
          totalTimeRemainingMinutes: (ride.durationMinutes || 0) % 60,
          totalCost: calculateRideCost(ride),
          co2Saved: (ride.distanceKm || 0) * (ride.type === 'bus' ? 0.15 : 0.2),
          timePeriod,
          transitType,
          updatedAt: FieldValue.serverTimestamp()
        });
      } else {
        // Increment existing stats
        const currentStats = statsDoc.data();
        const newTotalMinutes = (currentStats.totalTimeMinutes || 0) + (ride.durationMinutes || 0);
        
        batch.update(statsRef, {
          totalDistance: FieldValue.increment(ride.distanceKm || 0),
          totalRides: FieldValue.increment(1),
          totalTimeMinutes: FieldValue.increment(ride.durationMinutes || 0),
          totalTimeHours: Math.floor(newTotalMinutes / 60),
          totalTimeRemainingMinutes: newTotalMinutes % 60,
          totalCost: FieldValue.increment(calculateRideCost(ride)),
          co2Saved: FieldValue.increment((ride.distanceKm || 0) * (ride.type === 'bus' ? 0.15 : 0.2)),
          updatedAt: FieldValue.serverTimestamp()
        });
      }
    }
  }
  
  await batch.commit();
  console.log(`⚡ Stats incremented instantly for user ${userId}`);
}

// Helper to check if ride is in time period
function isRideInTimePeriod(ride, timePeriod) {
  const rideTime = new Date(ride.startTime?.toDate ? ride.startTime.toDate() : ride.startTime);
  const now = new Date();
  
  switch (timePeriod) {
    case '1w': 
      const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      return rideTime >= oneWeekAgo;
    case '1m':
      const oneMonthAgo = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
      return rideTime >= oneMonthAgo;
    case '1y':
      const oneYearAgo = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
      return rideTime >= oneYearAgo;
    case 'ytd':
      const yearStart = new Date(now.getFullYear(), 0, 1);
      return rideTime >= yearStart;
    case 'allTime':
      return true;
    default:
      return false;
  }
}

// Helper to calculate ride cost
function calculateRideCost(ride) {
  // TODO: Add transfer logic here if needed
  return ride.type === 'bus' ? 2.25 : 2.5;
}

function thirtyDaysAgo() {
  return new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
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
exports.smartRideNotificationSweep = onSchedule("every 20 minutes", async (event) => {
  console.log("🚀 Running smart ride notification sweep...");

  const snapshot = await db.collection("users").get();
  const now = Date.now();

  for (const userDoc of snapshot.docs) {
    const userData = userDoc.data();
    const userId = userDoc.id;

    if (!userData?.fcmToken) continue; // Skip if no device token

    const lastMotion = userData.lastMotionTimestamp?.toMillis?.() || 0;
    const isRiding = userData.isCurrentlyRiding || false;

    const minutesSinceLastMotion = (now - lastMotion) / (1000 * 60);

    let sentNotification = false;

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

  // Step 1: Get a valid trip for this route and direction
  const tripsSnapshot = await db.collection('trips')
    .where('route_id', '==', routeId)
    .where('direction_id', '==', directionId)
    .limit(1)
    .get();

  if (tripsSnapshot.empty) {
    throw new Error("No matching trip found.");
  }

  const tripId = tripsSnapshot.docs[0].id;

  // Step 2: Get start and end stop_times by trip_id + stop_id
  const stopTimesSnapshot = await db.collection('stop_times')
    .where('trip_id', '==', tripId)
    .where('stop_id', 'in', [startStopId, endStopId])
    .get();

  if (stopTimesSnapshot.empty || stopTimesSnapshot.size < 2) {
    throw new Error("Start or end stop not found in stop_times for this trip.");
  }

  // Step 3: Extract stop_time docs
  const stopTimes = stopTimesSnapshot.docs.map(doc => doc.data());
  const start = stopTimes.find(s => s.stop_id === startStopId);
  const end = stopTimes.find(s => s.stop_id === endStopId);

  if (!start || !end) {
    throw new Error("Could not match start or end stop from snapshot.");
  }

  const startSeq = parseInt(start.stop_sequence, 10);
  const endSeq = parseInt(end.stop_sequence, 10);

  if (isNaN(startSeq) || isNaN(endSeq)) {
    throw new Error("Invalid stop sequence data.");
  }

  if (startSeq >= endSeq) {
    throw new Error("End stop comes before start stop in sequence.");
  }

  const arrivalStart = parseTimeToSeconds(start.arrival_time);
  const arrivalEnd = parseTimeToSeconds(end.arrival_time);

  const durationSeconds = arrivalEnd - arrivalStart;
  const distanceKm = parseFloat(end.shape_dist_traveled || 0) - parseFloat(start.shape_dist_traveled || 0);

  if (durationSeconds <= 0 || distanceKm < 0) {
    throw new Error("Invalid timing or distance order.");
  }

  return {
    durationSeconds,
    distanceKm: Math.round(distanceKm * 1000) / 1000
  };
});

// Helper: Convert HH:MM:SS to seconds
function parseTimeToSeconds(timeStr) {
  if (!timeStr || typeof timeStr !== 'string') return 0;
  const [hours, minutes, seconds] = timeStr.split(':').map(Number);
  return (hours * 3600) + (minutes * 60) + (seconds || 0);
}

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

  // 🔥 FIXED: Write to user subcollection instead of root collection
  const rideRef = await db.collection('users').doc(userId).collection('rides').add(rideData);

  console.log(`🚀 Live ride started for user: ${userId}, rideId: ${rideRef.id}`);
  return { rideId: rideRef.id };
});

// Update an active live ride
exports.updateLiveRide = onCall(async (request) => {
  const { rideId, distanceIncrementMiles, timeIncrementSeconds, userId } = request.data;

  if (!rideId || distanceIncrementMiles == null || timeIncrementSeconds == null || !userId) {
    throw new Error('Missing required parameters.');
  }

  // 🔥 FIXED: Read from user subcollection instead of root collection
  const rideRef = db.collection('users').doc(userId).collection('rides').doc(rideId);
  const rideSnap = await rideRef.get();

  if (!rideSnap.exists) {
    throw new Error('Ride not found.');
  }

  const ride = rideSnap.data();

  if (!ride.inProgress) {
    throw new Error('Cannot update a completed ride.');
  }

  // Update distance and time
  await rideRef.update({
    distanceMiles: FieldValue.increment(distanceIncrementMiles),
    durationSeconds: FieldValue.increment(timeIncrementSeconds),
  });

  console.log(`⏱️ Ride updated: ${rideId} (+${distanceIncrementMiles} mi, +${timeIncrementSeconds}s)`);

  // 🔍 Check for false ride (after 10 min and under 0.1 miles)
  const totalDistance = (ride.distanceMiles || 0) + distanceIncrementMiles;
  const totalDuration = (ride.durationSeconds || 0) + timeIncrementSeconds;

  if (totalDuration > 600 && totalDistance < 0.1 && !ride.suspectedFalseRide) {
    await rideRef.update({ suspectedFalseRide: true });
    console.log(`⚠️ Suspected false ride flagged after 10 min: ${rideId}`);
  }

  return { success: true };
});

// End a live ride (finalize)
exports.endLiveRide = onCall(async (request) => {
  const { rideId, endStop, userId } = request.data;

  if (!rideId || !endStop || !userId) {
    throw new Error('Missing required parameters.');
  }

  // 🔥 FIXED: Read from user subcollection instead of root collection
  const rideRef = db.collection('users').doc(userId).collection('rides').doc(rideId);
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
  const { rideId, userId } = request.data;

  if (!rideId || !userId) {
    throw new Error('Missing required parameters.');
  }

  // 🔥 FIXED: Delete from user subcollection instead of root collection
  await db.collection('users').doc(userId).collection('rides').doc(rideId).delete();

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
  const achievementRef = userRef.collection('achievementsUnlocked').doc(achievementId);
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
      shown: false,
      unlockedAt: FieldValue.serverTimestamp()
    });
  }
};

// 🔥 NEW HELPER FUNCTION: Check and Unlock Achievements
async function checkAndUnlockAchievements(userId) {
  const userRef = db.collection('users').doc(userId);
  const snapshot = await userRef.collection('rides').get();
  const totalRides = snapshot.size;

  // Read stats from the correct subcollection location
  const statsDoc = await userRef.collection('stats').doc('allTime_all').get();
  const statsData = statsDoc.exists ? statsDoc.data() : {};
  
  const totalDistance = statsData.totalDistance || 0;
  const totalCO2 = statsData.co2Saved || 0;

  console.log(`🏆 Checking achievements for ${userId}: ${totalRides} rides, ${totalDistance} distance, ${totalCO2} CO2`);

  // Get the latest ride for specialty achievements
  const latestRideSnapshot = await userRef.collection('rides').orderBy('startTime', 'desc').limit(1).get();
  const ride = latestRideSnapshot.docs[0]?.data();

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

  // SPECIALTY ride-based achievements (only if there's a ride)
  if (ride) {
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
  }
}

// 🔥 NEW: Smart achievement checking - only check relevant ones
async function checkRelevantAchievements(userId, ride) {
  const userRef = db.collection('users').doc(userId);
  
  // Get current stats efficiently
  const statsDoc = await userRef.collection('stats').doc('allTime_all').get();
  const stats = statsDoc.exists ? statsDoc.data() : {};
  
  const totalRides = stats.totalRides || 0;
  const totalDistance = stats.totalDistance || 0;
  const totalCO2 = stats.co2Saved || 0;

  console.log(`🎯 Smart achievement check for ${userId}: ${totalRides} rides, ${totalDistance}km, ${totalCO2}kg CO2`);

  // RIDE COUNT - Only check the exact milestone we just hit
  const rideMilestones = {
    1: "getting_started",
    10: "getting_the_hang", 
    25: "city_commuter",
    50: "transit_regular",
    100: "transit_hero",
    250: "ultimate_rider"
  };
  
  if (rideMilestones[totalRides]) {
    await unlockAchievement(userId, rideMilestones[totalRides]);
  }

  // DISTANCE - Only check if we just crossed a threshold
  const distanceThresholds = [
    { km: 10, id: "warming_up" },
    { km: 25, id: "rolling_along" },
    { km: 50, id: "transit_star" },
    { km: 100, id: "transit_veteran" },
    { km: 250, id: "master_of_the_map" }
  ];

  for (const threshold of distanceThresholds) {
    const previousDistance = totalDistance - (ride.distanceKm || 0);
    if (previousDistance < threshold.km && totalDistance >= threshold.km) {
      await unlockAchievement(userId, threshold.id);
      break; // Only one distance achievement per ride
    }
  }

  // CO2 - Only check if we just crossed a threshold
  const co2Thresholds = [
    { kg: 10, id: "carbon_kicker" },
    { kg: 25, id: "eco_rider" },
    { kg: 50, id: "planet_mover" },
    { kg: 100, id: "green_machine" },
    { kg: 250, id: "sustainability_hero" }
  ];

  const rideCO2 = (ride.distanceKm || 0) * (ride.type === 'bus' ? 0.15 : 0.2);
  for (const threshold of co2Thresholds) {
    const previousCO2 = totalCO2 - rideCO2;
    if (previousCO2 < threshold.kg && totalCO2 >= threshold.kg) {
      await unlockAchievement(userId, threshold.id);
      break; // Only one CO2 achievement per ride
    }
  }

  // SPECIALTY achievements - only check these for the current ride
  if (ride) {
    const hour = new Date(ride.startTime?.toDate?.() || ride.startTime).getHours();
    
    // Time-based (only check if ride matches criteria)
    if (ride.wasLiveTracked && hour >= 23) {
      await unlockAchievement(userId, "night_owl");
    }
    if (ride.wasLiveTracked && hour < 6) {
      await unlockAchievement(userId, "early_bird");
    }

    // Route-based
    if (ride.startStop === ride.endStop) {
      await unlockAchievement(userId, "loop_de_loop");
    }

    // Stop count
    if (ride.stopCount === 1) {
      await unlockAchievement(userId, "one_stop_wonder");
    }
    if (ride.stopCount >= 15) {
      await unlockAchievement(userId, "scenic_route");
    }

    // Line completion - only check if new line
    if (ride.lineId && ride.type) {
      await checkLineCompletionAchievements(userId, ride.lineId, ride.type);
    }
  }
}

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

exports.onStreakUpdate = onDocumentWritten("users/{userId}", async (event) => {
  const userId = event.params.userId;
  const before = event.data?.before?.data();
  const after = event.data?.after?.data();

  if (!after) return;

  // 🔥 NEW: Prevent infinite loops from achievement updates
  // If this update is just from uiState or other non-streak fields, skip
  const streakFieldsChanged = 
    before?.currentStreak !== after?.currentStreak ||
    before?.longestStreak !== after?.longestStreak ||
    before?.lastRideDate !== after?.lastRideDate;
  
  if (!streakFieldsChanged) {
    console.log('⏭️ Skipping - no streak fields changed');
    return;
  }

  // Check if streak fields actually changed
  const beforeStreak = before?.currentStreak || 0;
  const afterStreak = after?.currentStreak || 0;

  // Only process if streak actually changed
  if (beforeStreak === afterStreak) return;

  const currentStreak = afterStreak;

  // Ride streak milestone achievements
  if (currentStreak === 3 && beforeStreak < 3) await unlockAchievementSafely(userId, "quick_streak");
  if (currentStreak === 7 && beforeStreak < 7) await unlockAchievementSafely(userId, "one_week_warrior");
  if (currentStreak === 14 && beforeStreak < 14) await unlockAchievementSafely(userId, "on_a_roll");
  if (currentStreak === 30 && beforeStreak < 30) await unlockAchievementSafely(userId, "cta_loyalist");
  if (currentStreak === 60 && beforeStreak < 60) await unlockAchievementSafely(userId, "unstoppable");
});

// 🔥 NEW: Safe achievement unlock that won't trigger user doc updates
async function unlockAchievementSafely(userId, achievementId) {
  const userRef = db.collection('users').doc(userId);
  const achievementRef = userRef.collection('achievementsUnlocked').doc(achievementId);
  const alreadyUnlocked = (await achievementRef.get()).exists;

  if (!alreadyUnlocked) {
    const globalRef = db.collection('achievements').doc(achievementId);
    const globalDoc = await globalRef.get();
    if (!globalDoc.exists) return;

    const { name, description, category } = globalDoc.data();

    // Only write to subcollections, not main user doc
    const batch = db.batch();
    
    // Write to achievements subcollection
    batch.set(achievementRef, {
      unlocked: true,
      unlockedAt: FieldValue.serverTimestamp(),
      name,
      description,
      category
    });

    // Write to UI state subcollection
    const uiStateRef = userRef.collection('uiState').doc('achievementPopup');
    batch.set(uiStateRef, {
      achievementId,
      name,
      description,
      category,
      shown: false,
      unlockedAt: FieldValue.serverTimestamp()
    });

    await batch.commit();
    console.log(`🏆 Safely unlocked ${achievementId} for ${userId}`);
  }
}

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

// ---------------- FIND NEARBY TRANSIT (AUTOFILL START STOP/LINE) ---------------- //

exports.findNearbyTransit = onCall(async (request) => {
  const { lat, lng, radiusMeters = 400 } = request.data;

  if (lat == null || lng == null) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing lat or lng');
  }

  const EARTH_RADIUS = 6371000; // meters

  function haversineDistance(lat1, lon1, lat2, lon2) {
    const toRad = (x) => (x * Math.PI) / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return EARTH_RADIUS * c;
  }

  // 🔥 NEW: Calculate geohash for user location
  const geohash = require('ngeohash');
  const userGeohash = geohash.encode(lat, lng, 6);
  const neighbors = geohash.neighbors(userGeohash);
  
  // Query nearby geohash cells (center + 8 neighbors)
  const geohashesToQuery = [userGeohash, ...Object.values(neighbors)];
  
  const nearbyStops = [];
  
  // 🔥 NEW: Query each geohash cell instead of ALL stops
  for (const hash of geohashesToQuery) {
    const snapshot = await db.collection('stops')
      .where('geohash', '>=', hash)
      .where('geohash', '<', hash + '\uf8ff')
      .get();
    
    // Add stops from this cell
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const stopLat = data.lat;
      const stopLon = data.lon;
      
      if (stopLat == null || stopLon == null) continue;
      
      // Still calculate exact distance for accuracy
      const distance = haversineDistance(lat, lng, stopLat, stopLon);
      if (distance > radiusMeters) continue;
      
      nearbyStops.push({
        stopId: doc.id,
        stopName: data.name || '',
        lat: stopLat,
        lon: stopLon,
        type: data.type || '',
        lineId: data.lineId || '',
        lineName: data.lineName || '',
        distanceMeters: distance,
      });
    }
  }

  if (nearbyStops.length === 0) {
    console.log(`⚠️ No stops found within ${radiusMeters}m of [${lat}, ${lng}]`);
    return { success: false, message: 'No nearby stops found.' };
  }

  // Sort stops by distance (closest first)
  nearbyStops.sort((a, b) => a.distanceMeters - b.distanceMeters);

  // Build set of unique lineIds among nearby stops
  const uniqueLineIds = new Set(nearbyStops.map(stop => stop.lineId));

  // Decide confidence
  let confidentLineId = null;
  let confidentLineName = null;
  let confidenceLevel = 'low';

  if (uniqueLineIds.size === 1) {
    confidentLineId = nearbyStops[0].lineId;
    confidentLineName = nearbyStops[0].lineName;
    confidenceLevel = 'high';
  }

  const bestStop = nearbyStops[0]; // closest one

  console.log(`📍 Nearby stop found: ${bestStop.stopName} (${bestStop.lineName}) — Confidence: ${confidenceLevel}`);
  console.log(`📊 Queried ${geohashesToQuery.length} cells, found ${nearbyStops.length} stops within ${radiusMeters}m`);

  return {
    success: true,
    transitType: bestStop.type,
    startStopId: bestStop.stopId,
    startStopName: bestStop.stopName,
    lineId: confidenceLevel === 'high' ? confidentLineId : null,
    lineName: confidenceLevel === 'high' ? confidentLineName : null,
    confidence: confidenceLevel,
    distanceMeters: Math.round(bestStop.distanceMeters)
  };
});

// ---------------- DELETE ACCOUNT ---------------- //

exports.deleteAccount = onCall(async (request) => {
  const uid = request.auth?.uid;

  if (!uid) {
    throw new functions.https.HttpsError('unauthenticated', 'User must be authenticated.');
  }

  const userRef = db.collection('users').doc(uid);

  // List of known subcollections to clean up
  const subcollections = [
    'rides',
    'achievementsUnlocked',
    'recentSearches',
    'fallbackSearches',
    'stats',
    'leaderboardStats',
    'settings',
    'detailStats',
    'uiState',
    'linesUsed'
  ];

  for (const sub of subcollections) {
    const snap = await userRef.collection(sub).get();
    const batch = db.batch();
    snap.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
  }

  // Delete the main user document
  await userRef.delete();

  // Delete the Firebase Auth user
  await admin.auth().deleteUser(uid);

  console.log(`🗑️ Deleted account and all data for user ${uid}`);
  return true; // ✅ return Boolean instead of object
});

// ---------------- CREATE DEFAULT SETTINGS ON USER SIGNUP ---------------- //

exports.onUserCreated = onDocumentCreated("users/{userId}", async (event) => {
  const userId = event.params.userId;
  const userRef = db.collection("users").doc(userId);
  const settingsRef = userRef.collection("settings").doc("preferences");

  await settingsRef.set({
    rideSettings: {
      distanceUnits: "miles",
      autofillEnabled: true,
    },
    gpsSettings: {
      backgroundTrackingEnabled: true,
    },
    notificationSettings: {
      rideReminders: true,
    }
  });

  console.log(`⚙️ Initialized default settings for new user: ${userId}`);
});

// 🔥 ADD THIS RIGHT AFTER onUserCreated
exports.onSettingsUpdated = onDocumentUpdated("users/{userId}/settings/preferences", async (event) => {
  const before = event.data?.before?.data();
  const after = event.data?.after?.data();
  
  if (before?.rideSettings?.distanceUnits !== after?.rideSettings?.distanceUnits) {
    console.log(`📏 User ${event.params.userId} changed units to: ${after.rideSettings.distanceUnits}`);
  }
});

// ---------------- FIND NEARBY END STOP (AUTOFILL END STOP ON RIDE END) ---------------- //

exports.findNearbyEndStop = onCall(async (request) => {
  const { lat, lng, lineId, radiusMeters = 400 } = request.data;

  if (lat == null || lng == null || !lineId) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing lat, lng, or lineId');
  }

  const EARTH_RADIUS = 6371000; // meters

  function haversineDistance(lat1, lon1, lat2, lon2) {
    const toRad = (x) => (x * Math.PI) / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return EARTH_RADIUS * c;
  }

  // 🔥 NEW: Calculate geohash for user location
  const geohash = require('ngeohash');
  const userGeohash = geohash.encode(lat, lng, 6);
  const neighbors = geohash.neighbors(userGeohash);
  
  // Query nearby geohash cells
  const geohashesToQuery = [userGeohash, ...Object.values(neighbors)];
  
  const nearbyStops = [];
  
  // 🔥 NEW: Query each geohash cell with lineId filter
  for (const hash of geohashesToQuery) {
    const snapshot = await db.collection('stops')
      .where('lineId', '==', lineId)
      .where('geohash', '>=', hash)
      .where('geohash', '<', hash + '\uf8ff')
      .get();
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const stopLat = data.lat;
      const stopLon = data.lon;

      if (stopLat == null || stopLon == null) continue;

      const distance = haversineDistance(lat, lng, stopLat, stopLon);
      if (distance > radiusMeters) continue;

      nearbyStops.push({
        stopId: doc.id,
        stopName: data.name || '',
        distanceMeters: distance,
      });
    }
  }

  if (nearbyStops.length === 0) {
    console.log(`⚠️ No end stops found within ${radiusMeters}m of [${lat}, ${lng}] for line ${lineId}`);
    return { success: false, message: 'No nearby end stop found.' };
  }

  nearbyStops.sort((a, b) => a.distanceMeters - b.distanceMeters);
  const bestStop = nearbyStops[0];

  console.log(`🏁 End stop auto-filled: ${bestStop.stopName} (${bestStop.stopId}) – ${Math.round(bestStop.distanceMeters)}m`);
  console.log(`📊 Queried ${geohashesToQuery.length} cells for line ${lineId}`);

  return {
    success: true,
    endStopId: bestStop.stopId,
    endStopName: bestStop.stopName,
    confidence: 'high',
    distanceMeters: Math.round(bestStop.distanceMeters)
  };
});

// ---------------- CHECK USERNAME AVAILABILITY ---------------- //

exports.checkUsernameAvailability = onCall(async (request) => {
  const { username } = request.data;

  if (!username || typeof username !== 'string') {
    throw new functions.https.HttpsError('invalid-argument', 'Username is required.');
  }

  const lowerUsername = username.trim().toLowerCase();

  // Check profanity
  if (leoProfanity.check(lowerUsername) || customBannedUsernames.includes(lowerUsername)) {
    return {
      available: false,
      allowed: false,
      reason: 'inappropriate',
    };
  }

  // Check uniqueness
  const usersRef = admin.firestore().collection('users');
  const snapshot = await usersRef.where('username', '==', lowerUsername).limit(1).get();

  const isTaken = !snapshot.empty;

  return {
    available: !isTaken,
    allowed: true,
  };
});
