package com.alio.octopusio;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiOutputStream extends OutputStream {

	private ExecutorService mExecutor = Executors.newCachedThreadPool();

	private final ConcurrentMap<String, ConcurrentLinkedQueue<byte[]>> mMessageList = new ConcurrentHashMap<String, ConcurrentLinkedQueue<byte[]>>();

	private ConcurrentLinkedQueue<String> mMessageChannelQueue = new ConcurrentLinkedQueue<String>();

	private PipedInputStream mInputStream;

	private PipedOutputStream mOutputStream;

	private AtomicBoolean mHadClosed = new AtomicBoolean(false);

	public MultiOutputStream() {
		super();
		mOutputStream = new PipedOutputStream();
		try {
			mInputStream = new PipedInputStream(mOutputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		mExecutor.execute(new InputStreamReader(mInputStream));
	}

	public void appendOutputStream(OutputStream... outputStreams) {
		OutputStreamWriter cWriter = null;
		for (OutputStream outputStream : outputStreams) {
			cWriter = new OutputStreamWriter(outputStream);
			mExecutor.execute(cWriter);
			mMessageChannelQueue.add(cWriter.getName());
		}
	}

	private void appendMessage(String name, byte[] product) {
		ConcurrentLinkedQueue<byte[]> cProductQueue = null;
		if (mMessageList.containsKey(name)) {
			cProductQueue = mMessageList.get(name);
		} else {
			cProductQueue = new ConcurrentLinkedQueue<byte[]>();
			mMessageList.put(name, cProductQueue);
		}
		synchronized (cProductQueue) {
			cProductQueue.add(product);
			cProductQueue.notifyAll();
		}
	}

	private byte[] obtainMessage(String name) throws InterruptedException {
		ConcurrentLinkedQueue<byte[]> cProductQueue = null;
		if (mMessageList.containsKey(name)) {
			cProductQueue = mMessageList.get(name);
		} else {
			cProductQueue = new ConcurrentLinkedQueue<byte[]>();
			mMessageList.put(name, cProductQueue);
		}
		synchronized (cProductQueue) {
			if (cProductQueue.isEmpty()) {
				cProductQueue.wait();
			}
			return cProductQueue.remove();
		}
	}

	@Override
	public void write(int b) throws IOException {
		mOutputStream.write(b);
	}

	private synchronized void dispatchMessage(byte[] b, int offset, int len) {
		byte[] buffer = new byte[len];
		System.arraycopy(b, offset, buffer, 0, len);
		for (String channel : mMessageChannelQueue) {
			appendMessage(channel, buffer);
		}
	}

	private synchronized void writeFinished() {
		mExecutor.shutdownNow();
	}
	
	@Override
	public void close() throws IOException {
		super.close();
	}

	private class InputStreamReader implements Runnable {

		private InputStream mInputStream;

		public InputStreamReader(InputStream inputStream) {
			super();
			mInputStream = inputStream;
		}

		@Override
		public void run() {
			try {
				byte[] buffer = new byte[1024];
				int len = -1;
				while ((len = mInputStream.read(buffer)) != -1) {
					dispatchMessage(buffer, 0, len);
				}
			} catch (IOException e) {
				// e.printStackTrace();
				mHadClosed.set(true);
				writeFinished();
			} finally {
				try {
					mInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private class OutputStreamWriter implements Runnable {

		private OutputStream mOutputStream = null;

		private String mName;

		private OutputStreamWriter(OutputStream outputStream) {
			super();
			this.mOutputStream = outputStream;
			this.mName = this.mOutputStream.toString();
		}

		public String getName() {
			return mName;
		}

		@Override
		public void run() {
			byte[] buffer = null;
			try {
				while (true) {
					if (mOutputStream == null) {
						break;
					}
					if (mHadClosed.get()) {
						break;
					}
					buffer = obtainMessage(mName);
					mOutputStream.write(buffer);
				}
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		MultiOutputStream multiOutputStream = new MultiOutputStream();
		multiOutputStream.appendOutputStream(new FileOutputStream("file1.txt"));
		multiOutputStream.appendOutputStream(new FileOutputStream("file2.txt"));
		multiOutputStream.appendOutputStream(new FileOutputStream("file3.txt"));
		multiOutputStream.write("hello world\r\n".getBytes("utf-8"));
		multiOutputStream.write("132432232\r\n".getBytes("utf-8"));
		multiOutputStream.write("aefadbadf\r\n".getBytes("utf-8"));
		multiOutputStream.write("fawer\r\n".getBytes("utf-8"));
		multiOutputStream.close();
	}

}
