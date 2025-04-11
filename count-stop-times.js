const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const countStopTimes = async () => {
  try {
    const snapshot = await db.collection("stop_times").count().get();
    console.log(`🧮 Total stop_times documents: ${snapshot.data().count}`);
  } catch (error) {
    console.error("❌ Error running count query:", error.message);
  }
};

countStopTimes();
