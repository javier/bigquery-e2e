	package com.googlecode.bigquery_e2e.sensors.client;

import android.os.Bundle;
import android.os.IBinder;
import android.app.Activity;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.SharedPreferences;
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
import android.widget.TextView;

public class ManageActivity extends Activity {
	public final static String PREFS = "com.googlecode.bigquery_e23.sensors.client.prefs";
	public final static String DEVICE_ID_KEY = "device_id";
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
		monitoringToggle = (Switch) findViewById(R.id.monitoringToggle);
		freqSpinner = (Spinner) findViewById(R.id.frequencyControl);
		// Create an ArrayAdapter using the string array and a default spinner layout
		ArrayAdapter<CharSequence> adapter = ArrayAdapter.createFromResource(this,
		        R.array.freq_list, android.R.layout.simple_spinner_item);
		// Specify the layout to use when the list of choices appears
		adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
		// Apply the adapter to the spinner
		freqSpinner.setAdapter(adapter);
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

	private void setRegistrationState(SharedPreferences prefs) {
		deviceId = prefs.getString(DEVICE_ID_KEY, null);
		EditText idEditor = (EditText) findViewById(R.id.deviceIdField);
		EditText zipEditor = (EditText) findViewById(R.id.homeZipField);
		monitoringToggle.setEnabled(deviceId != null);
		if (deviceId == null) {
		    idEditor.setText("");
		    idEditor.setEnabled(true);
		    zipEditor.setEnabled(true);
			registerButton.setText(R.string.register_label);
		} else {
			idEditor.setText(deviceId);
		    idEditor.setEnabled(false);
		    zipEditor.setEnabled(false);
			registerButton.setText(R.string.unregister_label);
		}
		monitoringToggle.setChecked(prefs.getBoolean(MONITORING_STATE, false));
		freqSpinner.setSelection(prefs.getInt(MONITORING_FREQ, 0));
	}

	private void doRegistration() {
		SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
		SharedPreferences.Editor editor = prefs.edit();
		if (deviceId == null) {
			editor.putString(DEVICE_ID_KEY, "magic_id_for_testing_463");
		} else {
			editor.remove(DEVICE_ID_KEY);
			editor.remove(MONITORING_STATE);
			editor.remove(MONITORING_FREQ);
			if (service != null) {
				service.stop();
			}
			monitoringToggle.setChecked(false);
		}
		editor.apply();
		setRegistrationState(prefs);
	}

	@Override
	protected void onDestroy() {
		service = null;
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
