package com.alio.octopusio;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MultiInputStream extends InputStream {

	private Executor mExecutor = Executors.newCachedThreadPool();

	private PipedInputStream mInputStream;

	private PipedOutputStream mOutputStream;

	public MultiInputStream() {
		super();
		mOutputStream = new PipedOutputStream();
		mInputStream = new PipedInputStream();
		try {
			mInputStream.connect(mOutputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void appendInputStream(InputStream... inputStreams) {
		for (InputStream inputStream : inputStreams) {
			mExecutor.execute(new InputStreamReader(inputStream));
		}
	}

	@Override
	public int read() throws IOException {
		return mInputStream.read();
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
					mOutputStream.write(buffer, 0, len);
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
}
