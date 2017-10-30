package com.bigdata2017.smartcar.flume.interceptor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class CollectDayInterceptor implements Interceptor {

	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
		String eventBody = new String(event.getBody()) + "," + new SimpleDateFormat("yyyyMMdd").format (new Date( System.currentTimeMillis() ) );
		event.setBody(eventBody.getBytes());
		return event;
	}


	@Override
	public void close() {
	}

	
	@Override
	public List<Event> intercept(List<Event> events) {
		for (Event event:events) {
			intercept(event);
		}
		return events;
	}
	

	public static class Builder implements Interceptor.Builder{
		@Override
		public void configure(Context context) {
		}

		@Override
		public Interceptor build() {
			return new CollectDayInterceptor();
		}
	}
}
