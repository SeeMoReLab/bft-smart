/**
 Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package bftsmart.communication.server;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.VMMessage;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.util.TOMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.*;
import java.io.*;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class represents a connection with other server.
 * ServerConnections are created by ServerCommunicationLayer.
 *
 * @author alysson
 */
public class ServerConnection {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final long POOL_TIME = 5000;
	private static final long WRITE_STALL_TIMEOUT_MS = 5000;
	private static final long WRITE_STALL_WATCHDOG_SLEEP_MS = 100;
	private final ServerViewController
			controller;
	private SSLSocket socket;
	private DataOutputStream socketOutStream = null;
	private DataInputStream socketInStream = null;
	private final int remoteId;
	private final boolean useSenderThread;
	protected LinkedBlockingQueue<OutboundServerMessage> outQueue;// = new LinkedBlockingQueue<OutboundServerMessage>(SEND_QUEUE_SIZE);
	private final LinkedBlockingQueue<SystemMessage> inQueue;

	private final Lock connectLock = new ReentrantLock();
	/** Only used when there is no sender Thread */
	private Lock sendLock;
	private boolean doWork = true;
	private volatile boolean writeInProgress = false;
	private volatile long writeStartMs = 0;
	private volatile long writeStartNanos = 0;
	private volatile long writeEpoch = 0;
	private volatile long lastWatchdogHandledWriteEpoch = -1;
	private volatile OutboundServerMessage currentWriteMessage = null;
	private final AtomicBoolean reconnectInProgress = new AtomicBoolean(false);

	private SecretKey secretKey = null;

	/**
	 * Tulio A. Ribeiro
	 * TLS vars.
	 */
	private KeyStore ks = null;
	private FileInputStream fis = null;
	private SSLSocketFactory socketFactory;
	private static final String SECRET = "MySeCreT_2hMOygBwY";

	public ServerConnection(ServerViewController controller,
							SSLSocket socket, int remoteId,
							LinkedBlockingQueue<SystemMessage> inQueue,
							ServiceReplica replica) {

		this.controller = controller;

		this.socket = socket;

		this.remoteId = remoteId;

		this.inQueue = inQueue;

		this.outQueue = new LinkedBlockingQueue<>(this.controller.getStaticConf().getOutQueueSize());

		// Connect to the remote process or just wait for the connection?
		if (isToConnect()) {
			ssltlsCreateConnection();
		}

		if (this.socket != null) {
			try {
				socketOutStream = new DataOutputStream(this.socket.getOutputStream());
				socketInStream = new DataInputStream(this.socket.getInputStream());
			} catch (IOException ex) {
				logger.error("Error creating connection to " + remoteId, ex);
			}
		}

		//******* EDUARDO BEGIN **************//
		this.useSenderThread = this.controller.getStaticConf().isUseSenderThread();

		if (useSenderThread && (this.controller.getStaticConf().getTTPId() != remoteId)) {
			new SenderThread().start();
		} else {
			sendLock = new ReentrantLock();
		}
		new WriteStallWatchdog().start();

		if (!this.controller.getStaticConf().isTheTTP()) {
			if (this.controller.getStaticConf().getTTPId() == remoteId) {
				//Uma thread "diferente" para as msgs recebidas da TTP
				new TTPReceiverThread(replica).start();
			} else {
				new ReceiverThread().start();
			}
		}
		//******* EDUARDO END **************//
	}
	/**
	 * Tulio A. Ribeiro.
	 * @return SecretKey
	 */
	public SecretKey getSecretKey() {
		if (secretKey != null)
			return secretKey;
		else {
			SecretKeyFactory fac;
			PBEKeySpec spec;
			try {
				fac = TOMUtil.getSecretFactory();
				spec = TOMUtil.generateKeySpec(SECRET.toCharArray());
				secretKey = fac.generateSecret(spec);
			} catch (NoSuchAlgorithmException |InvalidKeySpecException e) {
				logger.error("Algorithm error.",e);
			}
		}
		return secretKey;
	}

	/**
	 * Stop message sending and reception.
	 */
	public void shutdown() {
		logger.debug("SHUTDOWN for "+remoteId);

		doWork = false;
		closeSocket(true);
	}

	/**
	 * Used to send packets to the remote server.
	 */
	public final void send(byte[] data) throws InterruptedException {
		send(OutboundServerMessage.raw(data));
	}

	/**
	 * Used to send packets to the remote server.
	 */
	public final void send(OutboundServerMessage outboundMessage) throws InterruptedException {
		if (useSenderThread) {
			// only enqueue messages if there queue is not full
			if (!outQueue.offer(outboundMessage)) {
				logger.debug("Out queue for " + remoteId + " full (message discarded).");
			}
		} else {
			sendLock.lock();
			sendBytes(outboundMessage);
			sendLock.unlock();
		}
	}

	/**
	 * try to send a message through the socket if some problem is detected, a
	 * reconnection is done
	 */
	private final void sendBytes(OutboundServerMessage outboundMessage) {
		boolean abort = false;
		byte[] messageData = outboundMessage.payload();
		do {
			if (abort) {
				logger.info("Aborting send of message to {} (message={})", remoteId, outboundMessage.describeForDequeueLog());
				return; // if there is a need to reconnect, abort this method
			}
			if (socket != null && socketOutStream != null) {
				long writeStartWallMsSnapshot = System.currentTimeMillis();
				long writeStartNanosSnapshot = System.nanoTime();
				long writeEpochSnapshot = onWriteStart(outboundMessage, writeStartWallMsSnapshot, writeStartNanosSnapshot);
				try {
					// do an extra copy of the data to be sent, but on a single out stream write
					byte[] data = new byte[5 + messageData.length];// without MAC
					int value = messageData.length;

					System.arraycopy(new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8),
							(byte) value }, 0, data, 0, 4);
					System.arraycopy(messageData, 0, data, 4, messageData.length);
					System.arraycopy(new byte[] { (byte) 0 }, 0, data, 4 + messageData.length, 1);

					socketOutStream.write(data);

					return;
				} catch (IOException ex) {
					long writeDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeStartNanosSnapshot);
					logger.info(
							"Write failed while sending to {} (writeEpoch={}, durationMs={}, message={}); closing socket and reconnecting",
							remoteId,
							writeEpochSnapshot,
							writeDurationMs,
							outboundMessage.describeForDequeueLog());
					if (outboundMessage.shouldPreserveAcrossReconnect()) {
						if (outQueue.offer(outboundMessage)) {
							logger.info("Re-queued in-flight outbound message for replica {} after send failure (message={})",
									remoteId,
									outboundMessage.describeForDequeueLog());
						} else {
							logger.warn("Could not re-queue in-flight outbound message for replica {} because queue is full (message={})",
									remoteId,
									outboundMessage.describeForDequeueLog());
						}
					}
					closeSocket(false);
					waitAndConnect("sender write failure", ex);
					abort = true;
				} catch (RuntimeException ex) {
					long writeDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeStartNanosSnapshot);
					logger.warn(
							"Unexpected runtime failure while sending to {} (writeEpoch={}, durationMs={}, message={}); closing socket and reconnecting",
							remoteId,
							writeEpochSnapshot,
							writeDurationMs,
							outboundMessage.describeForDequeueLog(),
							ex);
					if (outboundMessage.shouldPreserveAcrossReconnect()) {
						if (outQueue.offer(outboundMessage)) {
							logger.info("Re-queued in-flight outbound message for replica {} after runtime send failure (message={})",
									remoteId,
									outboundMessage.describeForDequeueLog());
						} else {
							logger.warn("Could not re-queue in-flight outbound message for replica {} because queue is full (message={})",
									remoteId,
									outboundMessage.describeForDequeueLog());
						}
					}
					closeSocket(false);
					waitAndConnect("sender runtime failure", ex);
					abort = true;
				} finally {
					onWriteEnd();
				}
			} else {
				logger.info("Missing socket or output stream for replica {} when trying to send message {}; waiting for reconnection",
						remoteId,
						outboundMessage.describeForDequeueLog());
				waitAndConnect("sender missing socket/output stream", null);
				abort = true;
			}
		} while (doWork);
	}

	//******* EDUARDO BEGIN **************//
	//return true of a process shall connect to the remote process, false otherwise
	private boolean isToConnect() {
		if (this.controller.getStaticConf().getTTPId() == remoteId) {
			//Need to wait for the connection request from the TTP, do not tray to connect to it
			return false;
		} else if (this.controller.getStaticConf().getTTPId() == this.controller.getStaticConf().getProcessId()) {
			//If this is a TTP, one must connect to the remote process
			return true;
		}
		boolean ret = false;
		if (this.controller.isInCurrentView()) {

			//in this case, the node with higher ID starts the connection
			if (this.controller.getStaticConf().getProcessId() > remoteId) {
				ret = true;
			}

			/* JCS: I commented the code below to fix a bug, but I am not sure
			 whether its completely useless or not. The 'if' above was taken
			 from that same code (its the only part I understand why is necessary)
			 I keep the code commented just to be on the safe side



			 boolean me = this.controller.isInLastJoinSet(this.controller.getStaticConf().getProcessId());
			 boolean remote = this.controller.isInLastJoinSet(remoteId);

			 //either both endpoints are old in the system (entered the system in a previous view),
			 //or both entered during the last reconfiguration
			 if ((me && remote) || (!me && !remote)) {
			 //in this case, the node with higher ID starts the connection
			 if (this.controller.getStaticConf().getProcessId() > remoteId) {
			 ret = true;
			 }
			 //this process is the older one, and the other one entered in the last reconfiguration
			 } else if (!me && remote) {
			 ret = true;

			 } //else if (me && !remote) { //this process entered in the last reconfig and the other one is old
			 //ret=false; //not necessary, as ret already is false
			 //}

			 */
		}
		return ret;
	}
	//******* EDUARDO END **************//


	/**
	 * (Re-)establish connection between peers.
	 *
	 * @param newSocket socket created when this server accepted the connection
	 * (only used if processId is less than remoteId)
	 */
	protected void reconnect(SSLSocket newSocket) {

		connectLock.lock();

		if (socket == null || !socket.isConnected()) {

			if (isToConnect()) {
				ssltlsCreateConnection();
			} else {
				socket = newSocket;
			}

			if (socket != null) {
				try {
					socketOutStream = new DataOutputStream(socket.getOutputStream());
					socketInStream = new DataInputStream(socket.getInputStream());

					// authKey = null;
					// authenticateAndEstablishAuthKey();
				} catch (IOException ex) {
					logger.error("Failed to authenticate to replica", ex);
				}
			}
		}

		connectLock.unlock();
	}


	private void closeSocket(boolean flushOutputStream) {

		connectLock.lock();

		if (socket != null) {
			SSLSocket socketToClose = socket;
			try {
				if (flushOutputStream && socketOutStream != null) {
					socketOutStream.flush();
				}
				if (!flushOutputStream) {
					// Forced-close path (e.g., stalled write watchdog): use abortive
					// teardown to unblock in-flight write as fast as possible.
					try {
						socketToClose.setSoLinger(true, 0);
					} catch (SocketException se) {
						logger.debug("Could not set SO_LINGER=0 for replica {}", remoteId, se);
					}
					try {
						socketToClose.shutdownOutput();
					} catch (IOException | RuntimeException ex) {
						logger.debug("Output shutdown failed for replica {}", remoteId, ex);
					}
					try {
						socketToClose.shutdownInput();
					} catch (IOException | RuntimeException ex) {
						logger.debug("Input shutdown failed for replica {}", remoteId, ex);
					}
				}
				socketToClose.close();
			} catch (IOException ex) {
				logger.debug("Error closing socket to "+remoteId);
			} catch (NullPointerException npe) {
				logger.debug("Socket already closed");
			}

			socket = null;
			socketOutStream = null;
			socketInStream = null;
		}

		connectLock.unlock();
	}

	private long onWriteStart(OutboundServerMessage outboundMessage, long startWallMs, long startNanos) {
		currentWriteMessage = outboundMessage;
		writeStartMs = startWallMs;
		writeStartNanos = startNanos;
		long nextWriteEpoch = writeEpoch + 1;
		writeEpoch = nextWriteEpoch;
		writeInProgress = true;
		return nextWriteEpoch;
	}

	private void onWriteEnd() {
		writeInProgress = false;
		currentWriteMessage = null;
		writeStartMs = 0;
		writeStartNanos = 0;
	}

	private void waitAndConnect() {
		waitAndConnect("unspecified", null);
	}

	private void waitAndConnect(String reason, Throwable cause) {
		if (!doWork) {
			return;
		}

		if (!reconnectInProgress.compareAndSet(false, true)) {
			logger.debug(
					"Reconnect already in progress for replica {} (reason={}); skipping duplicate request",
					remoteId,
					reason);
			return;
		}

		try {
			if (!doWork) {
				return;
			}

			int queuedBeforeSleep = outQueue.size();
			if (cause != null) {
				logger.info(
						"Reconnect requested for replica {} (reason={}, queuedOutMessages={})",
						remoteId,
						reason,
						queuedBeforeSleep);
				logger.info("Reconnect cause for replica {}", remoteId, cause);
			} else {
				logger.info(
						"Reconnect requested for replica {} (reason={}, queuedOutMessages={})",
						remoteId,
						reason,
						queuedBeforeSleep);
			}

			try {
				Thread.sleep(POOL_TIME);
			} catch (InterruptedException ie) {
				logger.error("Failed to sleep", ie);
			}

			List<OutboundServerMessage> drained = new ArrayList<>(outQueue.size());
			outQueue.drainTo(drained);

			List<OutboundServerMessage> preserved = new ArrayList<>();
			int dropped = 0;
			for (OutboundServerMessage queued : drained) {
				if (queued.shouldPreserveAcrossReconnect()) {
					preserved.add(queued);
				} else {
					dropped++;
				}
			}

			if (dropped > 0) {
				logger.info(
						"Clearing {} queued outbound message(s) before reconnect to replica {}",
						dropped,
						remoteId);
			}
			if (!preserved.isEmpty()) {
				logger.info(
						"Preserving {} queued state-transfer outbound message(s) across reconnect to replica {}",
						preserved.size(),
						remoteId);

				int keepStateRequestIndex = -1;
				int keepStateRequestCid = -1;
				for (int i = 0; i < preserved.size(); i++) {
					OutboundServerMessage queued = preserved.get(i);
					if (!queued.isStateTransferRequest()) {
						continue;
					}
					int cid = queued.getCid();
					if (keepStateRequestIndex == -1 || cid >= keepStateRequestCid) {
						keepStateRequestIndex = i;
						keepStateRequestCid = cid;
					}
				}

				if (keepStateRequestIndex != -1) {
					List<OutboundServerMessage> dedupedPreserved = new ArrayList<>(preserved.size());
					int droppedStateRequests = 0;
					for (int i = 0; i < preserved.size(); i++) {
						OutboundServerMessage queued = preserved.get(i);
						if (queued.isStateTransferRequest()) {
							if (i == keepStateRequestIndex) {
								dedupedPreserved.add(queued);
							} else {
								droppedStateRequests++;
							}
							continue;
						}
						dedupedPreserved.add(queued);
					}

					if (droppedStateRequests > 0) {
						logger.info(
								"Deduplicated {} preserved SM_REQUEST message(s) before reconnect to replica {} (keptCid={})",
								droppedStateRequests,
								remoteId,
								keepStateRequestCid);
					}
					preserved = dedupedPreserved;
				}
			}

			reconnect(null);
			if (socket != null && socket.isConnected() && !socket.isClosed()) {
				logger.info("Reconnect attempt to replica {} completed with active socket", remoteId);
			} else {
				logger.info("Reconnect attempt to replica {} did not establish active socket", remoteId);
			}
			if (!preserved.isEmpty()) {
				int restored = 0;
				for (OutboundServerMessage message : preserved) {
					if (outQueue.offer(message)) {
						restored++;
					} else {
						logger.warn(
								"Could not restore preserved outbound message after reconnect to replica {} because queue is full (message={})",
								remoteId,
								message.describeForDequeueLog());
					}
				}
				logger.info(
						"Restored {} preserved outbound message(s) after reconnect to replica {}",
						restored,
						remoteId);
			}
		} finally {
			reconnectInProgress.set(false);
		}
	}

	/**
	 * Thread used to send packets to the remote server.
	 */
	private class SenderThread extends Thread {

		public SenderThread() {
			super("Sender for " + remoteId);
		}

		@Override
		public void run() {
			logger.info("Sender for " + remoteId + " started!");

			OutboundServerMessage outboundMessage = null;

			while (doWork) {
				//get a message to be sent
				try {
					outboundMessage = outQueue.poll(POOL_TIME, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ex) {
					logger.error("Failed to poll message from outQueue", ex);
				}

				if (outboundMessage != null) {
					if (outboundMessage.shouldLogOnDequeue()) {
						logger.info(
								"Dequeued outbound {} to replica {} (remainingOutQueue={})",
								outboundMessage.describeForDequeueLog(),
								remoteId,
								outQueue.size());
					}
					logger.trace("Sending data to, RemoteId:{}", remoteId);
					try {
						sendBytes(outboundMessage);
					} catch (Throwable t) {
						logger.error("Sender thread hit unexpected throwable for replica {}; attempting recovery", remoteId, t);
						closeSocket(false);
						waitAndConnect("sender thread throwable", t);
					}
				}
			}

			logger.info("Sender for " + remoteId + " stopped!");
		}
	}

	/**
	 * Thread used to receive packets from the remote server.
	 */
	protected class ReceiverThread extends Thread {

		public ReceiverThread() {
			super("Receiver for " + remoteId);
		}

		@Override
		public void run() {

			while (doWork) {
				if (socket != null && socketInStream != null) {

					try {
						// read data length
						int dataLength = socketInStream.readInt();
						byte[] data = new byte[dataLength];

						// read data
						int read = 0;
						do {
							read += socketInStream.read(data, read, dataLength - read);
						} while (read < dataLength);

						byte hasMAC = socketInStream.readByte();

						logger.trace("Read: {}, HasMAC: {}", read, hasMAC);

						SystemMessage sm = (SystemMessage) (new ObjectInputStream(new ByteArrayInputStream(data))
								.readObject());

						//The verification it is done for the SSL/TLS protocol.
						sm.authenticated = true;

						if (sm.getSender() == remoteId) {
							if (!inQueue.offer(sm)) {
								logger.warn("Inqueue full (message from " + remoteId + " discarded).");
							}/* else {
								logger.trace("Message: {} queued, remoteId: {}", sm.toString(), sm.getSender());
							}*/
						}
					} catch (ClassNotFoundException ex) {
						logger.info("Invalid message received. Ignoring!");
					} catch (IOException ex) {
						if (doWork) {
							logger.info("Closing socket and reconnecting");
							closeSocket(true);
							waitAndConnect("receiver I/O failure", ex);
						}
					} catch (Exception ex) {
						logger.info("Processing message failed. Ignoring!");
					}
				} else {
					waitAndConnect("receiver missing socket/input stream", null);
				}
			}
		}
	}

	//******* EDUARDO BEGIN: special thread for receiving messages indicating the entrance into the system, coming from the TTP **************//
	// Simly pass the messages to the replica, indicating its entry into the system
	//TODO: Ask eduardo why a new thread is needed!!!
	//TODO2: Remove all duplicated code

	/**
	 * Thread used to receive packets from the remote server.
	 */
	protected class TTPReceiverThread extends Thread {

		private final ServiceReplica replica;

		public TTPReceiverThread(ServiceReplica replica) {
			super("TTPReceiver for " + remoteId);
			this.replica = replica;
		}

		@Override
		public void run() {

			while (doWork) {
				if (socket != null && socketInStream != null) {
					try {
						// read data length
						int dataLength = socketInStream.readInt();

						byte[] data = new byte[dataLength];

						// read data
						int read = 0;
						do {
							read += socketInStream.read(data, read, dataLength - read);
						} while (read < dataLength);

						SystemMessage sm = (SystemMessage) (new ObjectInputStream(new ByteArrayInputStream(data))
								.readObject());

						if (sm.getSender() == remoteId) {
							this.replica.joinMsgReceived((VMMessage) sm);
						}

					} catch (ClassNotFoundException ex) {
						logger.error("Failed to deserialize message", ex);
					} catch (IOException ex) {
						// ex.printStackTrace();
						if (doWork) {
							closeSocket(true);
							waitAndConnect("ttp receiver I/O failure", ex);
						}
					}
				} else {
					waitAndConnect("ttp receiver missing socket/input stream", null);
				}
			}
		}
	}
	//******* EDUARDO END **************//

	private class WriteStallWatchdog extends Thread {

		public WriteStallWatchdog() {
			super("WriteStallWatchdog for " + remoteId);
			setDaemon(true);
		}

		@Override
		public void run() {
			while (doWork) {
				try {
					Thread.sleep(WRITE_STALL_WATCHDOG_SLEEP_MS);
				} catch (InterruptedException ie) {
					logger.debug("Write stall watchdog sleep interrupted", ie);
				}

				if (!doWork || !writeInProgress) {
					continue;
				}

				long start = writeStartMs;
				if (start <= 0) {
					continue;
				}

				long startNanos = writeStartNanos;
				long elapsed = System.currentTimeMillis() - start;
				long elapsedMonotonicMs = (startNanos > 0)
						? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
						: -1;
				if (elapsed < WRITE_STALL_TIMEOUT_MS) {
					continue;
				}

				long observedWriteEpoch = writeEpoch;
				if (observedWriteEpoch == lastWatchdogHandledWriteEpoch) {
					continue;
				}
				lastWatchdogHandledWriteEpoch = observedWriteEpoch;
				OutboundServerMessage stalledMessage = currentWriteMessage;
				String stalledDescription = (stalledMessage != null)
						? stalledMessage.describeForDequeueLog()
						: "unknown";
				logger.warn(
						"Detected stalled outbound socket write to replica {} (sampledWriteEpoch={}, sampledStartMs={}, sampledStartNanos={}, elapsedWallMs={}, elapsedMonoMs={}, threshold={} ms, queuedOutMessages={}, message={}); forcing socket close",
						remoteId,
						observedWriteEpoch,
						start,
						startNanos,
						elapsed,
						elapsedMonotonicMs,
						WRITE_STALL_TIMEOUT_MS,
						outQueue.size(),
						stalledDescription);
				closeSocket(false);
			}

			logger.info("Write stall watchdog for {} stopped!", remoteId);
		}
	}


	/**
	 * Deal with the creation of SSL/TLS connection.
	 *  Author: Tulio A. Ribeiro
	 *
	 */

	public void ssltlsCreateConnection() {

		SecretKeyFactory fac;
		PBEKeySpec spec;
		try {
			fac = TOMUtil.getSecretFactory();
			spec = TOMUtil.generateKeySpec(SECRET.toCharArray());
			secretKey = fac.generateSecret(spec);
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			logger.error("Algorithm error.", e);
		}

		String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
		try {
			fis = new FileInputStream("config/keysSSL_TLS/" + this.controller.getStaticConf().getSSLTLSKeyStore());
			ks = KeyStore.getInstance(KeyStore.getDefaultType());
			ks.load(fis, SECRET.toCharArray());
		} catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
			logger.error("SSL connection error.",e);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					logger.error("IO error.",e);
				}
			}
		}
		try {
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
			kmf.init(ks, SECRET.toCharArray());

			TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(algorithm);
			trustMgrFactory.init(ks);
			SSLContext context = SSLContext.getInstance(this.controller.getStaticConf().getSSLTLSProtocolVersion());
			context.init(kmf.getKeyManagers(), trustMgrFactory.getTrustManagers(), new SecureRandom());
			socketFactory = context.getSocketFactory();

		} catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyManagementException e) {
			logger.error("SSL connection error.",e);
		}
		// Create the connection.
		try {
			this.socket = (SSLSocket) socketFactory.createSocket(this.controller.getStaticConf().getHost(remoteId),
					this.controller.getStaticConf().getServerToServerPort(remoteId));
			this.socket.setKeepAlive(true);
			this.socket.setTcpNoDelay(true);
			this.socket.setEnabledCipherSuites(this.controller.getStaticConf().getEnabledCiphers());

			this.socket.addHandshakeCompletedListener(new HandshakeCompletedListener() {
				@Override
				public void handshakeCompleted(HandshakeCompletedEvent event) {
					logger.info("SSL/TLS handshake complete!, Id:{}" + "  ## CipherSuite: {}.", remoteId,
							event.getCipherSuite());
				}
			});

			this.socket.startHandshake();

			ServersCommunicationLayer.setSSLSocketOptions(this.socket);
			new DataOutputStream(this.socket.getOutputStream())
					.writeInt(this.controller.getStaticConf().getProcessId());

		} catch (SocketException | UnknownHostException e) {
			logger.error("Connection refused", e);
		} catch (IOException e) {
			logger.error("IO error.",e);
		}

	}
}
