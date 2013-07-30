	package com.googlecode.bigquery_e2e.sensors.client;

import android.os.Bundle;
import android.os.IBinder;
import android.app.Activity;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.view.Menu;
import android.view.View;
import android.widget.AdapterView;
import android.widget.AdapterView.OnItemSelectedListener;
import android.widget.ArrayAdapter;
import android.widget.Spinner;
import android.widget.Switch;

public class ManageActivity extends Activity {
	private MonitoringService service;
	private Spinner freqSpinner;
	private Switch monitoringToggle;
	private final static int FREQUENCY[] = {
		1, 5, 10, 30, 60
	};

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
		freqSpinner.setOnItemSelectedListener(new OnItemSelectedListener() {
			@Override
			public void onItemSelected(AdapterView<?> adapter, View control,
					int position, long id) {
				onSettingsChange(null);
			}
			@Override
			public void onNothingSelected(AdapterView<?> adapter) {
				onSettingsChange(null);
			}
		});
		bindService(new Intent(this, MonitoringService.class), connection, Context.BIND_AUTO_CREATE);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.manage, menu);
		return true;
	}
	
	public void onSettingsChange(View view) {
		updateService();
	}
	
	private void updateService() {
		if (service != null) {
			if (monitoringToggle.isChecked()) {
				int index = freqSpinner.getSelectedItemPosition();
				if (index >= 0 && index < FREQUENCY.length) {
					service.start(FREQUENCY[index] * 60 * 1000);
					return;
				}
			}
			service.stop();
		}
	}

	
}
