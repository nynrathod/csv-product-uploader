const fs = require('fs');
const path = require('path');

const SOURCE_FILE = 'data.csv';
const TARGET_FILE = 'large_data_200k.csv';
const TARGET_ROWS = 200000;

function generate() {
    console.log(`Reading source file: ${SOURCE_FILE}...`);
    const content = fs.readFileSync(SOURCE_FILE, 'utf-8');
    const lines = content.split(/\r?\n/).filter(line => line.trim().length > 0);

    if (lines.length < 2) {
        console.error('Source file is empty or invalid.');
        return;
    }

    const header = lines[0]; // name;price;expiration
    const dataRows = lines.slice(1);

    console.log(`Source loaded: ${dataRows.length} rows.`);
    console.log(`Generating ${TARGET_FILE} with ${TARGET_ROWS} rows...`);

    const writeStream = fs.createWriteStream(TARGET_FILE);
    writeStream.write(header + '\n');

    let written = 0;
    while (written < TARGET_ROWS) {
        const randomRow = dataRows[Math.floor(Math.random() * dataRows.length)];
        // Modify row slightly to ensure uniqueness if needed, or just exact copy?
        // User asked "form existing", so exact copy or random mix is fine.
        // We will just copy the line exactly to preserve the format validity.

        // Ensure valid draining of buffer to check backpressure
        const canWrite = writeStream.write(randomRow + '\n');
        written++;

        if (written % 10000 === 0) {
            process.stdout.write(`\rProgress: ${written}/${TARGET_ROWS} rows...`);
        }

        if (!canWrite) {
            // Handle backpressure if needed (though for 200k rows synchronously it might be fine on modern SSD, but let's be safe-ish or just sync blocking for simplicity in script)
            // Actually, simply looping is fine for 200k lines in Node unless buffer explodes. 200k * ~100 bytes = 20MB. It fits in RAM easily.
        }
    }

    writeStream.end(() => {
        console.log('\nDone!');
        console.log(`File created: ${TARGET_FILE}`);
    });
}

generate();
