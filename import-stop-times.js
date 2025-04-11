const admin = require("firebase-admin");
const fs = require("fs");
const csv = require("csv-parser");

const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const importStopTimes = async () => {
  const stopTimes = [];

  fs.createReadStream("stop_times.txt")
    .pipe(csv())
    .on("data", (row) => {
      const stopTime = {
        trip_id: row.trip_id,
        arrival_time: row.arrival_time,
        departure_time: row.departure_time,
        stop_id: row.stop_id,
        stop_sequence: parseInt(row.stop_sequence),
        pickup_type: row.pickup_type ? parseInt(row.pickup_type) : 0,
        drop_off_type: row.drop_off_type ? parseInt(row.drop_off_type) : 0,
      };
      stopTimes.push(stopTime);
    })
    .on("end", async () => {
      console.log(`🕒 Importing ${stopTimes.length} stop_times to Firestore...`);

      const BATCH_LIMIT = 500;
      for (let i = 0; i < stopTimes.length; i += BATCH_LIMIT) {
        const batch = db.batch();
        const chunk = stopTimes.slice(i, i + BATCH_LIMIT);

        chunk.forEach((stopTime) => {
          const docRef = db.collection("stop_times").doc(); // Auto-generated ID
          batch.set(docRef, stopTime);
        });

        await batch.commit();
        console.log(`✅ Imported ${Math.min(i + BATCH_LIMIT, stopTimes.length)} of ${stopTimes.length}`);
      }

      console.log("🎉 All stop_times successfully imported!");
    });
};

importStopTimes();
