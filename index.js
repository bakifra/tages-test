const fs = require("fs");
const path = require("path");
const readline = require("readline");

const CHUNK_SIZE = 400 * 1024 * 1024; // 400 МБ
const INPUT_FILE = "input.txt";
const OUTPUT_FILE = "output.txt";
const TEMP_DIR = "temp";

function validate(filePath, expectedExtension = ".txt") {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Файл ${filePath} не существует.`);
  }

  const fileExtension = path.extname(filePath);
  if (fileExtension !== expectedExtension) {
    throw new Error(
      `Файл ${filePath} должен иметь расширение ${expectedExtension}.`
    );
  }
  if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR);
  }

  if (fs.existsSync(OUTPUT_FILE)) {
    fs.writeFileSync(OUTPUT_FILE, "");
  }
}

// Функция для сортировки и сохранения чанка
function sortAndSaveChunk(lines, chunkIndex) {
  lines.sort();
  const tempFilePath = path.join(TEMP_DIR, `chunk_${chunkIndex}.txt`);
  fs.writeFileSync(tempFilePath, lines.join("\n"));
}

async function splitAndSortChunks() {
  try {
    // Проверяем входной файл
    validate(INPUT_FILE, ".txt");

    const stream = fs.createReadStream(INPUT_FILE, {
      highWaterMark: CHUNK_SIZE,
    });
    const rl = readline.createInterface({ input: stream });

    let lines = [];
    let chunkIndex = 0;
    let currentSize = 0;

    for await (const line of rl) {
      lines.push(line);
      currentSize += Buffer.byteLength(line);

      if (currentSize >= CHUNK_SIZE) {
        sortAndSaveChunk(lines, chunkIndex);
        lines = [];
        currentSize = 0;
        chunkIndex++;
      }
    }

    // Сохраняем последний чанк, если он не пустой
    if (lines.length > 0) {
      sortAndSaveChunk(lines, chunkIndex);
    }
  } catch (error) {
    console.error("Ошибка при разбиении файла на чанки:", error.message);
    throw error;
  }
}

// Функция для слияния отсортированных чанков
async function mergeChunks() {
  const tempFiles = fs
    .readdirSync(TEMP_DIR)
    .map((file) => path.join(TEMP_DIR, file));
  const streams = tempFiles.map((file) =>
    readline.createInterface({ input: fs.createReadStream(file) })
  );

  const heap = [];
  const output = fs.createWriteStream(OUTPUT_FILE);

  // Инициализация кучи
  for (let i = 0; i < streams.length; i++) {
    const iterator = streams[i][Symbol.asyncIterator]();
    const { value } = await iterator.next();
    heap.push({ value, iterator, index: i });
  }

  heap.sort((a, b) => a.value.localeCompare(b.value));

  while (heap.length > 0) {
    const { value, iterator, index } = heap[0];
    output.write(`${value}\n`);

    const { value: nextValue, done } = await iterator.next();
    if (!done) {
      heap[0] = { value: nextValue, iterator, index };
    } else {
      heap.shift();
    }

    heap.sort((a, b) => a.value.localeCompare(b.value));
  }

  output.end();
}

async function main() {
  try {
    console.log("Разбиение и сортировка чанков...");
    await splitAndSortChunks();

    console.log("Слияние чанков...");
    await mergeChunks();

    console.log("Сортировка завершена.");
  } catch (error) {
    console.error("Ошибка:", error.message);
  }
}

main();
