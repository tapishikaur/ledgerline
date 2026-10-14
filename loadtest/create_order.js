// k6 load test: CreateOrder over gRPC at a constant arrival rate.
//
//   k6 run -e ADDR=orders:50051 -e RATE=300 -e DURATION=60s loadtest/create_order.js
//
// A constant *arrival* rate (not a fixed number of virtual users) is what "300 requests/s"
// means: k6 starts 300 requests every second whether or not earlier ones have finished, so a
// slow server shows up as latency, not as a quietly lower rate.
import grpc from 'k6/net/grpc';
import { check } from 'k6';
import exec from 'k6/execution';

const client = new grpc.Client();
client.load(['../proto'], 'ledgerline/v1/ledgerline.proto');

const RATE = parseInt(__ENV.RATE || '300');
const RUN = __ENV.RUN || `k6-${Date.now()}`;

export const options = {
  scenarios: {
    create_orders: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  if (exec.vu.iterationInScenario === 0) {
    client.connect(__ENV.ADDR || 'localhost:50051', { plaintext: true });
  }
  const n = exec.scenario.iterationInTest;
  const res = client.invoke('ledgerline.v1.Orders/CreateOrder', {
    order_id: `${RUN}-${n}`,
    customer_id: `c${n % 1000}`,
    amount_minor: 100 + (n * 7919) % 99900,
  });
  check(res, { 'status is OK': (r) => r && r.status === grpc.StatusOK });
}
