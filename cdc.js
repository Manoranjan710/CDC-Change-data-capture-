const ZongJi = require("zongji");
const { createClient } = require("redis");

const redis = createClient();

async function main() {
  await redis.connect();

  const zongji = new ZongJi({
    host: "localhost",
    port: 3306,
    user: "root",
    password: "Codot@12345",
  });

  zongji.on("binlog", async (event) => {
    const eventName = event.getEventName();

    console.log("⚡ Event detected:", event.getEventName());
    if (event.tableMap && event.tableMap[event.tableId]) {
      console.log("   ↳ Table:", event.tableMap[event.tableId].tableName);
      console.log("   ↳ Schema:", event.tableMap[event.tableId].parentSchema);
    }

    if (["writerows", "updaterows", "deleterows"].includes(eventName)) {
      const tableInfo = event.tableMap[event.tableId];
      if (!tableInfo) return;

      const { parentSchema: schema, tableName: table } = tableInfo;

      if (schema === "cloth_store" && table === "shirts") {
        for (const row of event.rows) {
          const data = {
            db: schema,
            table,
            eventName,
            row,
            timestamp: new Date().toISOString(),
          };

          await redis.xAdd("cloth_store_changes", "*", {
            change: JSON.stringify(data),
          });

          console.log("📩 Change captured and pushed to Redis:", data);
        }
      }
    }
  });

  zongji.start({
    serverId: 1,
    includeEvents: ["writerows", "updaterows", "deleterows"],
    includeSchema: { cloth_store: ["shirts"] },
    binlogName: "DESKTOP-U5MLFLU-bin.000007", // replace with your LAST binlog
    binlogNextPos: 4,
  });

  console.log("✅ Listening for changes in cloth_store.shirts...");
}

main().catch(console.error);
