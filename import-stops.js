const admin = require("firebase-admin");
const fs = require("fs");
const csv = require("csv-parser");

const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const importStops = async () => {
  const stops = [];

  fs.createReadStream("stops.txt")
    .pipe(csv())
    .on("data", (row) => {
      // You can customize fields here depending on what you need
      const stop = {
        id: row.stop_id,
        name: row.stop_name,
        lat: parseFloat(row.stop_lat),
        lon: parseFloat(row.stop_lon),
        locationType: row.location_type || "0",
        parentStation: row.parent_station || null,
      };
      stops.push(stop);
    })
    .on("end", async () => {
      console.log(`📍 Importing ${stops.length} stops to Firestore...`);

      const batch = db.batch();
      stops.forEach((stop) => {
        const ref = db.collection("stops").doc(stop.id);
        batch.set(ref, stop);
      });

      await batch.commit();
      console.log("✅ Stops successfully imported!");
    });
};

importStops();
