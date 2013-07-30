package com.googlecode.bigquery_e2e.sensors.client;

import java.util.Calendar;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.IntentService;
import android.content.Context;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

public class MonitoringService extends IntentService {
	private final static String TAG = "MonitoringService";

	public class Binder extends android.os.Binder {
		MonitoringService getService() {
			return MonitoringService.this;
		}
	}

	private final IBinder binder = new Binder(); 
	private PendingIntent pendingIntent;
	
	public MonitoringService() {
		super("Monitoring");
	}

	@Override
	public IBinder onBind(Intent intent) {
		return binder;
	}
	
	public void start(int intervalMillis) {
		stop();
		pendingIntent = PendingIntent.getService(
				this, 0, new Intent(this, MonitoringService.class), 0);
		AlarmManager alarm = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
		// Start every 30 seconds
		alarm.setRepeating(AlarmManager.RTC_WAKEUP, Calendar.getInstance().getTimeInMillis(),
				intervalMillis, pendingIntent); 
	}
	
	public void stop() {
		if (pendingIntent != null) {
			AlarmManager alarm = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
			alarm.cancel(pendingIntent);
			pendingIntent = null;
		}
	}

	@Override
	protected void onHandleIntent(Intent intent) {
		// TODO(siddartha): here is where the recording and logging ends up.
		Log.i(TAG, "Handling logging intent.");
	}
}
