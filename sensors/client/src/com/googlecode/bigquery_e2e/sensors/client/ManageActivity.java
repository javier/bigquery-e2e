package com.googlecode.bigquery_e2e.sensors.client;

import org.json.JSONException;
import org.json.JSONObject;

import android.app.Activity;
import android.app.ProgressDialog;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.SharedPreferences;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.os.IBinder;
import android.os.StatFs;
import android.util.DisplayMetrics;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemSelectedListener;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.CompoundButton;
import android.widget.CompoundButton.OnCheckedChangeListener;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.Switch;
import android.widget.Toast;

import com.googlecode.bigquery_e2e.sensors.client.CommandRunner.ErrorResult;

public class ManageActivity extends Activity {
	public final static String PREFS = "com.googlecode.bigquery_e23.sensors.client.prefs";
	public final static String DEVICE_ID_KEY = "device_id";
	public final static String HOST_KEY = "host_index";
	public final static String MONITORING_STATE = "monitoring_state";
	public final static String MONITORING_FREQ = "monitoring_freq";
	private final static int FREQUENCY[] = {
		10, 1 * 60, 5 * 60, 10 * 60, 30 * 60, 60 * 60
	};

	private MonitoringService service;
	private Spinner freqSpinner;
	private Switch monitoringToggle;
	private Button registerButton;
	private String deviceId;
    private ProgressDialog registeringDialog;

    private ServiceConnection connection = new ServiceConnection() {
		@Override
		public void onServiceConnected(ComponentName name, IBinder binder) {
			service = ((MonitoringService.Binder) binder).getService();
			updateService();
		}

		@Override
		public void onServiceDisconnected(ComponentName name) {
			service = null;
		}		
	};
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_manage);
		setupSpinner((Spinner) findViewById(R.id.hostControl), R.array.log_hosts);
		monitoringToggle = (Switch) findViewById(R.id.monitoringToggle);
		freqSpinner = (Spinner) findViewById(R.id.frequencyControl);
		setupSpinner(freqSpinner, R.array.freq_list);
		registerButton = (Button) findViewById(R.id.registerButton);
		registerButton.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				doRegistration();
			}
		});
		setRegistrationState(getSharedPreferences(PREFS, MODE_PRIVATE));
		monitoringToggle.setOnCheckedChangeListener(new OnCheckedChangeListener() {
			@Override
			public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
				updateService();
			}
		});
		freqSpinner.setOnItemSelectedListener(new OnItemSelectedListener() {
			@Override
			public void onItemSelected(AdapterView<?> adapter, View control,
					int position, long id) {
				updateService();
			}
			@Override
			public void onNothingSelected(AdapterView<?> adapter) {
				updateService();
			}
		});
		bindService(new Intent(this, MonitoringService.class), connection, Context.BIND_AUTO_CREATE);
	}

	private void setupSpinner(Spinner spinner, int listId) {
		ArrayAdapter<CharSequence> adapter = ArrayAdapter.createFromResource(this,
		        listId, android.R.layout.simple_spinner_item);
		adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
		spinner.setAdapter(adapter);		
	}

	private void setRegistrationState(SharedPreferences prefs) {
		deviceId = prefs.getString(DEVICE_ID_KEY, null);
		EditText idEditor = (EditText) findViewById(R.id.deviceIdField);
		EditText zipEditor = (EditText) findViewById(R.id.homeZipField);
		Spinner hostSpinner = (Spinner) findViewById(R.id.hostControl);
		monitoringToggle.setEnabled(deviceId != null);
		if (deviceId == null) {
		    idEditor.setText("");
		    idEditor.setEnabled(true);
		    zipEditor.setEnabled(true);
		    hostSpinner.setEnabled(true);
			registerButton.setText(R.string.register_label);
		} else {
			idEditor.setText(deviceId);
		    idEditor.setEnabled(false);
		    zipEditor.setEnabled(false);
		    hostSpinner.setEnabled(false);
			registerButton.setText(R.string.unregister_label);
		}
		monitoringToggle.setChecked(prefs.getBoolean(MONITORING_STATE, false));
		hostSpinner.setSelection(prefs.getInt(HOST_KEY, 0));
		freqSpinner.setSelection(prefs.getInt(MONITORING_FREQ, 0));
	}

	private void doRegistration() {		
		if (deviceId == null) {
			final String id = ((EditText) findViewById(R.id.deviceIdField)).getText().toString();
			final String zip = ((EditText) findViewById(R.id.homeZipField)).getText().toString();
			final String host = (String)((Spinner) findViewById(R.id.hostControl)).getSelectedItem();
			AsyncTask<Void, Void, CommandRunner.ErrorResult> task =
					new AsyncTask<Void, Void, CommandRunner.ErrorResult>() {
				@Override
				protected void onPreExecute() {
					super.onPreExecute();
					registeringDialog = new ProgressDialog(ManageActivity.this);
					registeringDialog.setTitle(R.string.registering);
					registeringDialog.setCancelable(false);
					registeringDialog.setIndeterminate(true);
					registeringDialog.show();
				}

				@Override
				protected CommandRunner.ErrorResult doInBackground(Void... params) {
					CommandRunner runner = new CommandRunner(host);
					JSONObject arg = new JSONObject();
					try {
						arg.put("id", id);
						arg.put("type", Build.DEVICE);
						arg.put("make", Build.MANUFACTURER);
						arg.put("model", Build.MODEL);
						arg.put("os", "android");
						arg.put("os_version", Build.VERSION.RELEASE);
						StatFs statFs = new StatFs(Environment.getRootDirectory().getAbsolutePath());   
				        arg.put("storage_gb", ((double) statFs.getBlockCount() * statFs.getBlockSize()) /
				        		(1024 * 1024 * 1024));
				        DisplayMetrics metrics = new DisplayMetrics();
				        getWindowManager().getDefaultDisplay().getMetrics(metrics);
				        arg.put("resolution", metrics.widthPixels + "x" + metrics.heightPixels);
				        float width = metrics.widthPixels / metrics.xdpi;
				        float height = metrics.heightPixels / metrics.ydpi;
				        arg.put("screen_size", Math.sqrt(width * width + height * height));
						arg.put("zip", zip);
						runner.run("register", arg);
					} catch (JSONException e) {
						return new CommandRunner.ErrorResult(e);
					} catch (ErrorResult e) {
						return e;
					}
					return null;
				}

				@Override
				protected void onPostExecute(CommandRunner.ErrorResult result) {
					super.onPostExecute(result);
					if (registeringDialog != null) {
						if (result == null) {
							// No error occurred so the device is registered.
							SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
							SharedPreferences.Editor editor = prefs.edit();
							editor.putString(DEVICE_ID_KEY, id);
							editor.putInt(HOST_KEY,
									((Spinner) findViewById(R.id.hostControl)).getSelectedItemPosition());
							editor.apply();
							setRegistrationState(prefs);
						} else {
							Toast.makeText(getApplicationContext(),
									"Failed: " + result.getMessage(), Toast.LENGTH_SHORT).show();
						}
						registeringDialog.dismiss();
						registeringDialog = null;
					}
				}
			};
			task.execute();
		} else {
			SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
			SharedPreferences.Editor editor = prefs.edit();
			editor.remove(DEVICE_ID_KEY);
			editor.remove(MONITORING_STATE);
			editor.remove(MONITORING_FREQ);
			editor.apply();
			setRegistrationState(prefs);
			if (service != null) {
				service.stop();
			}
			monitoringToggle.setChecked(false);
		}
	}

	@Override
	protected void onDestroy() {
		service = null;
		if (registeringDialog != null) {
			registeringDialog.dismiss();
			registeringDialog = null;
		}
		super.onDestroy();
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.manage, menu);
		return true;
	}
	
	private void updateService() {
		SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
		SharedPreferences.Editor editor = prefs.edit();
		editor.putBoolean(MONITORING_STATE, monitoringToggle.isChecked());
		editor.putInt(MONITORING_FREQ, freqSpinner.getSelectedItemPosition());
		editor.apply();
		if (service != null) {
			if (monitoringToggle.isChecked()) {
				assert deviceId != null;
				int index = freqSpinner.getSelectedItemPosition();
				if (index >= 0 && index < FREQUENCY.length) {
					service.start(deviceId, FREQUENCY[index] * 1000);
					return;
				}
			}
			service.stop();
		}
	}

	@Override
	public boolean onOptionsItemSelected(MenuItem item) {
		switch (item.getItemId()) {
			case R.id.action_last_log:
				startActivity(new Intent(this, LastLogActivity.class));
				return true;
			default:
				return super.onOptionsItemSelected(item);
		}
	}
}
