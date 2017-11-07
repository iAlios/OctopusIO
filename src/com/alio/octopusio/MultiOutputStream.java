package com.alio.octopusio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiOutputStream extends OutputStream {

	private Executor mExecutor = Executors.newCachedThreadPool();

	private final ConcurrentMap<String, ConcurrentLinkedQueue<byte[]>> mMessageList = new ConcurrentHashMap<String, ConcurrentLinkedQueue<byte[]>>();

	private ConcurrentLinkedQueue<String> mMessageChannelQueue = new ConcurrentLinkedQueue<String>();

	private PipedInputStream mInputStream;

	private PipedOutputStream mOutputStream;

	private AtomicBoolean mHadClosed = new AtomicBoolean(false);
	
	public MultiOutputStream() {
		super();
		mOutputStream = new PipedOutputStream();
		mInputStream = new PipedInputStream();
		try {
			mOutputStream.connect(mInputStream);
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

	/**
	 * 创建商品
	 * 
	 * @param name
	 * @param object
	 */
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

	/**
	 * 删除商品
	 * 
	 * @param name
	 * @param object
	 */
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

	@Override
	public void close() throws IOException {
		mHadClosed.set(true);
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
					if(mHadClosed.get()) {
						break;
					}
					dispatchMessage(buffer, 0, len);
				}
			} catch (IOException e) {
				e.printStackTrace();
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
					if(mOutputStream == null) {
						break;
					}
					if(mHadClosed.get()) {
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

}
