package com.googlecode.bigquery_e2e.sensors.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import android.app.ActivityManager;
import android.app.ActivityManager.MemoryInfo;
import android.app.AlarmManager;
import android.app.IntentService;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.location.LocationManager;
import android.os.AsyncTask;
import android.os.BatteryManager;
import android.os.Debug;
import android.os.IBinder;
import android.os.PowerManager;
import android.util.Log;

import com.googlecode.bigquery_e2e.sensors.client.CommandRunner.ErrorResult;

public class MonitoringService extends IntentService {
	public final static String LOG_UPDATE = "com.googlecode.bigquery_e23.sensors.client.log_update";
	private final static String TAG = "MonitoringService";
	private static final String CURRENT_LOG = "log";
	private static final long MAX_LOG_SIZE = 1024 * 1024;
	private static final String LAST_LOG = "log.0";
	private Geocoder geocoder;
	private Intent logIntent;
	private String deviceId;
	private CommandRunner commandRunner;
	private JSONObject lastRecord;

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

	public void start(String deviceId, int intervalMillis,
			CommandRunner commandRunner) {
		stop();
		this.deviceId = deviceId;
		this.commandRunner = commandRunner;
		if (Geocoder.isPresent()) {
			geocoder = new Geocoder(this, Locale.getDefault());
		}
		// Prime location service.
		LocationManager manager = (LocationManager) getSystemService(LOCATION_SERVICE);
		manager.requestSingleUpdate(LocationManager.GPS_PROVIDER, PendingIntent
				.getService(this, 0, new Intent(this, MonitoringService.class), 0));
		if (logIntent == null) {
			logIntent = new Intent(this, MonitoringService.class);
			logIntent.setAction(Intent.ACTION_ATTACH_DATA);
		}
		pendingIntent = PendingIntent.getService(this, 0, logIntent, 0);
		AlarmManager alarm = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
		alarm.setRepeating(AlarmManager.RTC_WAKEUP, Calendar.getInstance()
				.getTimeInMillis(), intervalMillis, pendingIntent);
	}

	public void stop() {
		if (pendingIntent != null) {
			AlarmManager alarm = (AlarmManager) getSystemService(Context.ALARM_SERVICE);
			alarm.cancel(pendingIntent);
			pendingIntent = null;
		}
		deviceId = null;
	}

	JSONObject getLog() {
		return lastRecord;
	}

	@Override
	protected void onHandleIntent(Intent intent) {
		if (intent.filterEquals(logIntent)) {
			try {
				JSONObject newRecord = buildRecord();
				appendToLog(newRecord);
				lastRecord = newRecord;
				Intent update = new Intent(LOG_UPDATE);
				sendBroadcast(update);
				transmit(newRecord);
			} catch (JSONException ex) {
				Log.e(TAG, "Failed to build JSON record.", ex);
			} catch (IOException ex) {
				Log.e(TAG, "Could not save record.", ex);
			}
		}
	}

	private void transmit(final JSONObject record) {
		AsyncTask<Void, Void, Void> task = new AsyncTask<Void, Void, Void>() {
			@Override
			protected Void doInBackground(Void... params) {
				try {
					commandRunner.run("record", record);
				} catch (ErrorResult e) {
					Log.e(TAG, e.getError() + ": " + e.getMessage());
				}
				return null;
			}
		};
		task.execute();
	}

	private JSONObject buildRecord() throws JSONException {
		JSONObject newRecord = new JSONObject();
		newRecord.put("id", deviceId);
		newRecord.put("ts",
				((double) Calendar.getInstance().getTimeInMillis()) / 1000.0);
		newRecord.put("screen_on",
				((PowerManager) getSystemService(Context.POWER_SERVICE)).isScreenOn());
		newRecord.put("power", getPowerStatus());
		ActivityManager activityManager = (ActivityManager) getSystemService(ACTIVITY_SERVICE);
		newRecord.put("memory", getMemory(activityManager));
		newRecord.put("location", getLocation());
		newRecord.put("running", getRunning(activityManager));
		return newRecord;
	}

	private JSONObject getPowerStatus() throws JSONException {
		JSONObject power = new JSONObject();
		IntentFilter ifilter = new IntentFilter(Intent.ACTION_BATTERY_CHANGED);
		Intent batteryStatus = registerReceiver(null, ifilter);
		int status = batteryStatus.getIntExtra(BatteryManager.EXTRA_STATUS, -1);
		if (status != -1) {
			power.put("charging", status == BatteryManager.BATTERY_STATUS_CHARGING);
		}
		int chargePlug = batteryStatus
				.getIntExtra(BatteryManager.EXTRA_PLUGGED, -1);
		if (chargePlug != -1) {
			power.put("usb", chargePlug == BatteryManager.BATTERY_PLUGGED_USB);
			power.put("ac", chargePlug == BatteryManager.BATTERY_PLUGGED_AC);
		}
		int level = batteryStatus.getIntExtra(BatteryManager.EXTRA_LEVEL, -1);
		int scale = batteryStatus.getIntExtra(BatteryManager.EXTRA_SCALE, -1);
		if (level != -1 && scale != -1) {
			power.put("charge", level / (double) scale);
		}
		return power;
	}

	private JSONObject getMemory(ActivityManager activityManager)
			throws JSONException {
		MemoryInfo meminfo = new MemoryInfo();
		activityManager.getMemoryInfo(meminfo);
		JSONObject memory = new JSONObject();
		memory.put("available", meminfo.availMem);
		memory.put("low", meminfo.lowMemory);
		memory.put("used", meminfo.totalMem - meminfo.availMem);
		return memory;
	}

	private JSONObject getLocation() throws JSONException {
		JSONObject location = new JSONObject();
		LocationManager manager = (LocationManager) getSystemService(LOCATION_SERVICE);
		Location passive = manager
				.getLastKnownLocation(LocationManager.PASSIVE_PROVIDER);
		if (passive != null) {
			location.put("ts", passive.getTime() / 1000.0);
			location.put("provider", passive.getProvider());
			if (passive.hasAccuracy()) {
				location.put("accuracy", passive.getAccuracy());
			}
			location.put("lat", passive.getLatitude());
			location.put("lng", passive.getLongitude());
			if (passive.hasAltitude()) {
				location.put("altitude", passive.getAltitude());
			}
			if (passive.hasBearing()) {
				location.put("bearing", passive.getBearing());
			}
			if (passive.hasSpeed()) {
				location.put("speed", passive.getSpeed());
			}
			if (geocoder != null) {
				try {
					List<Address> addresses = geocoder.getFromLocation(
							passive.getLatitude(), passive.getLongitude(), 1);
					if (!addresses.isEmpty()) {
						Address address = addresses.get(0);
						if (address.getCountryCode() != null) {
							location.put("country", address.getCountryCode());
						}
						if (address.getAdminArea() != null) {
							location.put("state", address.getAdminArea());
						}
						if (address.getPostalCode() != null) {
							location.put("zip", address.getPostalCode());
						}
					}
				} catch (IOException e) {
					Log.w(TAG, "Failed to geocode location", e);
				}
			}
		}
		return location;
	}

	private JSONArray getRunning(ActivityManager activityManager)
			throws JSONException {
		JSONArray running = new JSONArray();
		List<ActivityManager.RunningAppProcessInfo> apps = activityManager
				.getRunningAppProcesses();
		int[] pids = new int[apps.size()];
		int index = 0;
		for (ActivityManager.RunningAppProcessInfo app : apps) {
			JSONObject entry = new JSONObject();
			entry.put("name", app.processName);
			pids[index++] = app.pid;
			if (app.pid != 0) {
				entry.put("pid", app.pid);
			}
			entry.put("uid", app.uid);
			if (app.lastTrimLevel > 0) {
				entry.put("memory_trim", app.lastTrimLevel);
			}
			entry.put("importance", getImportance(app));
			entry.put("package", new JSONArray(Arrays.asList(app.pkgList)));
			running.put(entry);
		}
		Debug.MemoryInfo[] appMemory = activityManager.getProcessMemoryInfo(pids);
		for (int i = 0; i < appMemory.length; i++) {
			if (appMemory[i] != null) {
				running.getJSONObject(i).put("memory", getAppMemory(appMemory[i]));
			}
		}
		return running;
	}

	private JSONObject getAppMemory(Debug.MemoryInfo memoryInfo)
			throws JSONException {
		JSONObject memory = new JSONObject();
		memory.put("total", memoryInfo.getTotalPss());
		memory.put("dirty_private", memoryInfo.getTotalPrivateDirty());
		memory.put("dirty_shared", memoryInfo.getTotalSharedDirty());
		return memory;
	}

	private JSONObject getImportance(ActivityManager.RunningAppProcessInfo app)
			throws JSONException {
		JSONObject importance = new JSONObject();
		importance.put("level", app.importance);
		importance.put("reason", app.importanceReasonCode);
		importance.put("lru", app.lru);
		if (app.importanceReasonPid != 0) {
			importance.put("pid", app.importanceReasonPid);
		}
		if (app.importanceReasonComponent != null) {
			importance.put("component",
					app.importanceReasonComponent.flattenToString());
		}
		return importance;
	}

	private void appendToLog(JSONObject record) throws IOException {
		File currentLog = new File(getCacheDir(), CURRENT_LOG);
		if (currentLog.exists() && currentLog.length() > MAX_LOG_SIZE) {
			File lastLog = new File(getCacheDir(), LAST_LOG);
			if (lastLog.exists()) {
				if (!lastLog.delete()) {
					Log.e(TAG, "Could not delete old log file: " + lastLog.getPath());
					return;
				}
			}
			if (!currentLog.renameTo(lastLog)) {
				Log.e(TAG, "Could not rotate: " + currentLog.getPath());
				return;
			}
		}
		FileOutputStream log = new FileOutputStream(currentLog, true);
		log.write(record.toString().getBytes());
		log.write('\n');
		log.close();
	}
}
