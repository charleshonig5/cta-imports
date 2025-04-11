const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

async function addDefaultSettingsToUsers() {
  const usersSnapshot = await db.collection("users").get();

  for (const userDoc of usersSnapshot.docs) {
    const userId = userDoc.id;

    const defaultSettings = {
      profile: {
        username: "", // You can prefill from userDoc if stored
        photoURL: "",
        email: "", // Optional if syncing from Firebase Auth
      },
      rideSettings: {
        autofillAssistance: true,
        distanceMetric: "miles",
      },
      gpsSettings: {
        backgroundTracking: true,
        locationPermission: true,
      },
      notifications: {
        rideReminders: true,
      },
    };

    await db
      .collection("users")
      .doc(userId)
      .collection("settings")
      .doc("preferences")
      .set(defaultSettings);

    console.log(`✅ Settings added for user: ${userId}`);
  }

  console.log("🎉 All settings initialized.");
}

addDefaultSettingsToUsers().catch(console.error);
