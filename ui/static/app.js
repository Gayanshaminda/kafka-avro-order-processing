const elements = {
  connection: document.querySelector("#connectionStatus"),
  orders: document.querySelector("#ordersMetric"),
  processed: document.querySelector("#processedMetric"),
  retries: document.querySelector("#retryMetric"),
  dlq: document.querySelector("#dlqMetric"),
  average: document.querySelector("#averageMetric"),
  total: document.querySelector("#totalMetric"),
  chart: document.querySelector("#productChart"),
  events: document.querySelector("#eventStream"),
  eventCount: document.querySelector("#eventCount"),
  dlqTable: document.querySelector("#dlqTable"),
  demoButton: document.querySelector("#demoButton"),
  resetButton: document.querySelector("#resetButton"),
  toast: document.querySelector("#toast"),
  pipeline: document.querySelector("#pipeline"),
};

let lastEventId = 0;
let toastTimer;

function currency(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => elements.toast.classList.remove("show"), 2600);
}

function renderChart(products) {
  elements.chart.replaceChildren();
  if (!products.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Run the demo to populate price averages.";
    elements.chart.append(empty);
    return;
  }

  const maxValue = Math.max(...products.map((product) => product.average), 1);
  products.forEach((product) => {
    const row = document.createElement("div");
    row.className = "bar-row";

    const label = document.createElement("div");
    label.className = "bar-label";
    const name = document.createElement("strong");
    name.textContent = product.name;
    const count = document.createElement("span");
    count.textContent = `${product.count} order${product.count === 1 ? "" : "s"}`;
    label.append(name, count);

    const track = document.createElement("div");
    track.className = "bar-track";
    const fill = document.createElement("div");
    fill.className = "bar-fill";
    fill.style.width = `${Math.max(5, (product.average / maxValue) * 100)}%`;
    track.append(fill);

    const value = document.createElement("div");
    value.className = "bar-value";
    value.textContent = currency(product.average);
    row.append(label, track, value);
    elements.chart.append(row);
  });
}

function renderEvents(events) {
  elements.events.replaceChildren();
  elements.eventCount.textContent = `${events.length} event${events.length === 1 ? "" : "s"}`;
  events.slice(0, 12).forEach((event) => {
    const item = document.createElement("div");
    item.className = `event-item ${event.kind}`;

    const indicator = document.createElement("span");
    indicator.className = "event-indicator";
    const copy = document.createElement("div");
    copy.className = "event-copy";
    const title = document.createElement("strong");
    title.textContent = event.orderId ? `${event.title} · ${event.orderId}` : event.title;
    const detail = document.createElement("span");
    detail.textContent = event.detail;
    copy.append(title, detail);
    const time = document.createElement("time");
    time.className = "event-time";
    time.textContent = event.time;
    item.append(indicator, copy, time);
    elements.events.append(item);
  });
}

function renderDlq(orders) {
  elements.dlqTable.replaceChildren();
  if (!orders.length) {
    const row = document.createElement("tr");
    row.className = "empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 6;
    cell.textContent = "No permanently failed messages.";
    row.append(cell);
    elements.dlqTable.append(row);
    return;
  }

  orders.forEach((order) => {
    const row = document.createElement("tr");
    const values = [order.orderId, order.product, currency(order.price)];
    values.forEach((content) => {
      const cell = document.createElement("td");
      cell.textContent = content;
      row.append(cell);
    });
    const reason = document.createElement("td");
    const pill = document.createElement("span");
    pill.className = "reason-pill";
    pill.textContent = order.reason;
    reason.append(pill);
    const attempts = document.createElement("td");
    attempts.textContent = order.attempts;
    const time = document.createElement("td");
    time.textContent = order.time;
    row.append(reason, attempts, time);
    elements.dlqTable.append(row);
  });
}

function flashPipeline(latestEvent) {
  document.querySelectorAll(".pipeline-node").forEach((node) => node.classList.remove("active"));
  if (!latestEvent || latestEvent.id === lastEventId) return;
  lastEventId = latestEvent.id;
  const stage = latestEvent.kind === "system" ? "broker" : "processor";
  const node = document.querySelector(`[data-stage="${stage}"]`);
  node?.classList.add("active");
  setTimeout(() => node?.classList.remove("active"), 700);
}

function render(state) {
  elements.connection.classList.toggle("online", state.connected);
  elements.connection.querySelector("span:last-child").textContent = state.connected
    ? "Kafka connected"
    : "Connecting to Kafka";
  elements.orders.textContent = state.ordersReceived;
  elements.processed.textContent = state.processed;
  elements.retries.textContent = state.retryEvents;
  elements.dlq.textContent = state.dlqCount;
  elements.average.textContent = Number(state.globalAverage).toFixed(2);
  elements.total.textContent = `${currency(state.globalTotal)} processed value`;
  renderChart(state.products);
  renderEvents(state.events);
  renderDlq(state.dlqOrders);
  flashPipeline(state.events[0]);
}

async function refresh() {
  try {
    const response = await fetch("/api/state", { cache: "no-store" });
    if (!response.ok) throw new Error("Dashboard API unavailable");
    render(await response.json());
  } catch (error) {
    elements.connection.classList.remove("online");
    elements.connection.querySelector("span:last-child").textContent = "Dashboard reconnecting";
  }
}

elements.demoButton.addEventListener("click", async () => {
  elements.demoButton.disabled = true;
  elements.demoButton.textContent = "Demo running…";
  showToast("Sending six Avro orders to Kafka");
  try {
    const response = await fetch("/api/demo", { method: "POST" });
    const result = await response.json();
    showToast(result.status === "completed" ? `Batch ${result.batch} sent successfully` : "A demo is already running");
  } catch (error) {
    showToast("Could not start the demo");
  } finally {
    elements.demoButton.disabled = false;
    elements.demoButton.innerHTML = '<span class="play-icon" aria-hidden="true"></span>Run live demo';
  }
});

elements.resetButton.addEventListener("click", async () => {
  await fetch("/api/reset", { method: "POST" });
  lastEventId = 0;
  showToast("Dashboard view cleared");
  await refresh();
});

refresh();
setInterval(refresh, 500);
