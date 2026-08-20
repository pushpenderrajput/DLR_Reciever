const cluster = require('cluster');
const os = require('os');

const numCPUs = process.env.WEB_CONCURRENCY || os.cpus().length || 4;

if (cluster.isPrimary) {
  let totalDlrs = 0;
  let sentCount = 0;
  let deliveredCount = 0;
  let readCount = 0;
  let failedCount = 0;
  let otherCount = 0;
  let lastCount = 0;

  console.log(`🚀 Primary process is starting ${numCPUs} worker threads...`);

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  // Aggregate metrics from worker processes
  for (const id in cluster.workers) {
    cluster.workers[id].on('message', (msg) => {
      if (msg.cmd === 'DLR_RECORDED') {
        totalDlrs++;
        if (msg.status === 'delivered') deliveredCount++;
        else if (msg.status === 'sent') sentCount++;
        else if (msg.status === 'read') readCount++;
        else if (msg.status === 'failed') failedCount++;
        else otherCount++;
      } else if (msg.cmd === 'STATS_REQUEST') {
        cluster.workers[msg.workerId].send({
          cmd: 'STATS_RESPONSE',
          requestId: msg.requestId,
          data: {
            totalReceived: totalDlrs,
            breakdown: {
              sent: sentCount,
              delivered: deliveredCount,
              read: readCount,
              failed: failedCount,
              other: otherCount
            },
            uptimeSeconds: Math.floor(process.uptime())
          }
        });
      } else if (msg.cmd === 'RESET_STATS') {
        totalDlrs = 0;
        sentCount = 0;
        deliveredCount = 0;
        readCount = 0;
        failedCount = 0;
        otherCount = 0;
      }
    });
  }

  cluster.on('exit', (worker) => {
    console.warn(`Worker ${worker.process.pid} died. Forking replacement...`);
    cluster.fork();
  });

  // Master console stat print every 5 seconds
  setInterval(() => {
    const diff = totalDlrs - lastCount;
    const currentTps = (diff / 5).toFixed(1);
    lastCount = totalDlrs;

    if (totalDlrs > 0) {
      console.log(
        `📊 [DLR Receiver Total] ${totalDlrs} | Speed: ~${currentTps} req/s | Sent: ${sentCount} | Delivered: ${deliveredCount} | Failed: ${failedCount}`
      );
    }
  }, 5000);

} else {
  // Worker Process
  const fastify = require('fastify')({
    logger: false,
    keepAliveTimeout: 75000,
    connectionTimeout: 10000,
    maxParamLength: 200
  });

  // High-performance payload parser
  fastify.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      const json = JSON.parse(body);
      done(null, json);
    } catch (err) {
      done(err, null);
    }
  });

  // Main Webhook Receiver Path
  fastify.post('/api/whatsapp/call-back/:uuid', async (request, reply) => {
    const body = request.body;
    let status = 'other';

    try {
      status = body?.entry?.[0]?.changes?.[0]?.value?.statuses?.[0]?.status || 'other';
    } catch (err) {
      status = 'other';
    }

    // Fire & forget metric to master
    process.send({ cmd: 'DLR_RECORDED', status });

    return reply.code(200).send({ status: 'ok' });
  });

  // Stats endpoints
  const pendingRequests = new Map();

  process.on('message', (msg) => {
    if (msg.cmd === 'STATS_RESPONSE') {
      const resolver = pendingRequests.get(msg.requestId);
      if (resolver) {
        resolver(msg.data);
        pendingRequests.delete(msg.requestId);
      }
    }
  });

  fastify.get('/stats', async (request, reply) => {
    const requestId = Math.random().toString(36).substring(2);
    return new Promise((resolve) => {
      pendingRequests.set(requestId, resolve);
      process.send({ cmd: 'STATS_REQUEST', workerId: cluster.worker.id, requestId });
    });
  });

  fastify.post('/stats/reset', async (request, reply) => {
    process.send({ cmd: 'RESET_STATS' });
    return { message: 'Metrics reset signal sent' };
  });

  fastify.get('/health', async () => ({ status: 'healthy' }));

  const port = process.env.PORT || 8080;
  fastify.listen({ port: Number(port), host: '0.0.0.0' }, (err) => {
    if (err) {
      console.error(err);
      process.exit(1);
    }
  });
}