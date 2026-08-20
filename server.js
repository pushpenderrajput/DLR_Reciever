const fastify = require('fastify')({
  logger: false // Disabled raw request logging to ensure maximum throughput
});

// Metrics state
let totalDlrs = 0;
let sentCount = 0;
let deliveredCount = 0;
let readCount = 0;
let failedCount = 0;
let otherCount = 0;
let lastCount = 0;

// Main DLR Callback Webhook Endpoint
fastify.post('/api/whatsapp/call-back/:uuid', async (request, reply) => {
  totalDlrs++;

  // Fast async extraction of status without blocking response
  const body = request.body;
  try {
    const status = body?.entry?.[0]?.changes?.[0]?.value?.statuses?.[0]?.status;
    if (status === 'delivered') deliveredCount++;
    else if (status === 'sent') sentCount++;
    else if (status === 'read') readCount++;
    else if (status === 'failed') failedCount++;
    else otherCount++;
  } catch (err) {
    otherCount++;
  }

  // Acknowledge immediately to prevent simulator socket timeouts
  return reply.code(200).send({ status: 'ok' });
});

// Live Stats Endpoint (Open in browser to check metrics during load tests)
fastify.get('/stats', async (request, reply) => {
  return {
    totalReceived: totalDlrs,
    breakdown: {
      sent: sentCount,
      delivered: deliveredCount,
      read: readCount,
      failed: failedCount,
      other: otherCount
    },
    uptimeSeconds: Math.floor(process.uptime())
  };
});

// Reset metrics endpoint
fastify.post('/stats/reset', async (request, reply) => {
  totalDlrs = 0;
  sentCount = 0;
  deliveredCount = 0;
  readCount = 0;
  failedCount = 0;
  otherCount = 0;
  return { message: 'Metrics reset successfully' };
});

// Health check endpoint for Render
fastify.get('/health', async (request, reply) => {
  return { status: 'healthy' };
});

// Periodic lightweight terminal progress log (Every 5 seconds)
setInterval(() => {
  const diff = totalDlrs - lastCount;
  const currentTps = (diff / 5).toFixed(1);
  lastCount = totalDlrs;

  if (totalDlrs > 0) {
    console.log(
      `📊 [DLR Sink] Total: ${totalDlrs} | Current Speed: ~${currentTps} DLR/s | Sent: ${sentCount} | Delivered: ${deliveredCount} | Failed: ${failedCount}`
    );
  }
}, 5000);

// Start Server on Render's assigned PORT
const port = process.env.PORT || 8080;
fastify.listen({ port: Number(port), host: '0.0.0.0' }, (err, address) => {
  if (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
  console.log(`🚀 DLR Sink Server running on ${address}`);
});