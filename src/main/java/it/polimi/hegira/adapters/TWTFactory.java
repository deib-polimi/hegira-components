package it.polimi.hegira.adapters;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TWTFactory implements ThreadFactory {

	private final AtomicInteger integer = new AtomicInteger(0);
	
	public TWTFactory() {
	}

	@Override
	public Thread newThread(Runnable r) {
		return new Thread(r, "" + integer.getAndIncrement());
	}

}
