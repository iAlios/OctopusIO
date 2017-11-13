package com.alio.octopusio;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiInputStream extends InputStream {

	private static final int MIN_BUFFER_SIZE = 1024;

	public static interface IBufferListener {
		byte[] onFilter(byte[] buf, int offset, int size);
	}

	private enum Status {
		UNINITALIZED, INITALIZED, RUNNING, FINISHED
	}

	private ExecutorService mExecutor = null;

	private PipedInputStream mInputStream;

	private PipedOutputStream mOutputStream;

	private IOException mInterruptedException = null;

	private AtomicBoolean mHadInterruptedByException = new AtomicBoolean(false);

	private AtomicBoolean mHadFinished = new AtomicBoolean(false);

	private Status mStatus = Status.UNINITALIZED;

	private final ConcurrentLinkedQueue<byte[]> mMessageQueue = new ConcurrentLinkedQueue<byte[]>();

	private ConcurrentLinkedQueue<InputStreamReader> mInputStreamReaderQueue = new ConcurrentLinkedQueue<>();

	private int mMinBufferSize = MIN_BUFFER_SIZE;

	private IBufferListener mBufferListener = null;

	public MultiInputStream() {
		super();
		try {
			mOutputStream = new PipedOutputStream();
			mInputStream = new PipedInputStream(mOutputStream);
			mStatus = Status.INITALIZED;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setMinBufferSize(int bufferSize) {
		mMinBufferSize = bufferSize;
	}

	public void appendInputStream(InputStream... inputStreams) {
		synchronized (mInputStreamReaderQueue) {
			if(mExecutor == null) {
				mExecutor = Executors.newCachedThreadPool();
				mExecutor.execute(new OutputStreamWriter(mOutputStream));
			}
			InputStreamReader cInputStreamReader = null;
			for (InputStream inputStream : inputStreams) {
				cInputStreamReader = new InputStreamReader(inputStream);
				cInputStreamReader.setInputReaderListener(mInputReaderListener);
				mExecutor.execute(cInputStreamReader);
				mInputStreamReaderQueue.add(cInputStreamReader);
			}
			mHadFinished.set(false);
		}
	}

	public void setMessageListener(IBufferListener cBufferListener) {
		mBufferListener = cBufferListener;
	}

	private void appendMessage(byte[] buf, int offset, int len) {
		synchronized (mMessageQueue) {
			byte[] buffer = new byte[len];
			System.arraycopy(buf, offset, buffer, 0, len);
			if (mBufferListener != null) {
				mBufferListener.onFilter(buffer, 0, len);
			}
			mMessageQueue.add(buffer);
			mMessageQueue.notifyAll();
		}
	}

	private byte[] obtainMessage() throws InterruptedException {
		synchronized (mMessageQueue) {
			if (mMessageQueue.isEmpty()) {
				if (mStatus == Status.FINISHED) {
					return null;
				}
				mMessageQueue.wait();
			}
			try {
				return mMessageQueue.remove();
			} catch (NoSuchElementException e) {
				return null;
			}
		}
	}

	IInputReaderListener mInputReaderListener = new IInputReaderListener() {

		@Override
		public void onError(InputStreamReader cInputStreamReader, IOException e) {
			mHadInterruptedByException.set(true);
			mInterruptedException = e;
		}

		@Override
		public void onFinished(InputStreamReader cInputStreamReader) {
			synchronized (mInputStreamReaderQueue) {
				mInputStreamReaderQueue.remove(cInputStreamReader);
				if (mInputStreamReaderQueue.isEmpty()) {
					mStatus = Status.FINISHED;
					synchronized (mMessageQueue) {
						mMessageQueue.notifyAll();
					}
					mExecutor.shutdown();
				}
			}
		}

		@Override
		public void onRead(byte[] buf, int offset, int size) {
			appendMessage(buf, offset, size);
		}

	};

	@Override
	public int read() throws IOException {
		if (mHadInterruptedByException.get()) {
			throw mInterruptedException;
		}
		try {
			return mInputStream.read();
		} catch (IOException e) {
			mInputStream.close();
			return -1;
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		mExecutor.shutdown();
	}
	
	private interface IInputReaderListener {
		void onError(InputStreamReader cInputStreamReader, IOException e);

		void onRead(byte[] buf, int offset, int size);

		void onFinished(InputStreamReader cInputStreamReader);
	}

	private class InputStreamReader implements Runnable {

		private InputStream mInputStream;

		private IInputReaderListener mListener = null;

		public InputStreamReader(InputStream inputStream) {
			super();
			mInputStream = inputStream;
		}

		public InputStreamReader setInputReaderListener(IInputReaderListener cListener) {
			mListener = cListener;
			return this;
		}

		@Override
		public void run() {
			try {
				byte[] buffer = new byte[mMinBufferSize];
				int len = -1;
				mStatus = Status.RUNNING;
				while ((len = mInputStream.read(buffer)) != -1) {
					if (mHadInterruptedByException.get()) {
						break;
					}
					if (mListener != null) {
						mListener.onRead(buffer, 0, len);
					}
				}
				if (mListener != null) {
					mListener.onFinished(this);
				}
			} catch (IOException e) {
				if (mListener != null) {
					mListener.onError(this, e);
				}
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

		private OutputStreamWriter(OutputStream outputStream) {
			super();
			this.mOutputStream = outputStream;
		}

		@Override
		public void run() {
			byte[] buf = null;
			try {
				while (true) {
					if (mOutputStream == null) {
						break;
					}
					buf = obtainMessage();
					if (buf == null) {
						break;
					}
					mOutputStream.write(buf);
				}
			} catch (InterruptedException | IOException e) {
				// e.printStackTrace();
			} finally {
				try {
					mOutputStream.close();
				} catch (IOException e) {
					// e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		MultiInputStream mInputStream = new MultiInputStream();
		mInputStream.appendInputStream(new FileInputStream("file1.txt"));
		mInputStream.appendInputStream(new FileInputStream("file2.txt"));
		mInputStream.appendInputStream(new FileInputStream("file3.txt"));
		BufferedReader bufferReader = new BufferedReader(new java.io.InputStreamReader(mInputStream));
		String line = null;
		while ((line = bufferReader.readLine()) != null) {
			System.out.println("====readLine====" + line);
		}
		bufferReader.close();
	}
}
