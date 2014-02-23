package com.googlecode.bigquery_e2e.sensors.client;

import org.json.JSONException;
import org.json.JSONObject;

import android.os.Bundle;
import android.os.IBinder;
import android.app.Activity;
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.ServiceConnection;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;

public class LastLogActivity extends Activity {
  private MonitoringService service;

  private ServiceConnection connection = new ServiceConnection() {
    @Override
    public void onServiceConnected(ComponentName name, IBinder binder) {
      service = ((MonitoringService.Binder) binder).getService();
      receiver.onReceive(null, null);
    }

    @Override
    public void onServiceDisconnected(ComponentName name) {
      service = null;
    }
  };

  private BroadcastReceiver receiver = new BroadcastReceiver() {
    @Override
    public void onReceive(Context context, Intent intent) {
      if (service != null) {
        JSONObject log = service.getLog();
        if (log != null) {
          TextView logView = (TextView) findViewById(R.id.log_data_view);
          try {
            logView.setText(log.toString(2));
          } catch (JSONException ex) {
            logView.setText(ex.toString());
          }
        }
      }
    }
  };

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_last_log);
    TextView logView = (TextView) findViewById(R.id.log_data_view);
    logView.setMovementMethod(ScrollingMovementMethod.getInstance());
    bindService(new Intent(this, MonitoringService.class), connection,
      Context.BIND_AUTO_CREATE);
  }

  @Override
  protected void onDestroy() {
    service = null;
    super.onDestroy();
  }

  @Override
  protected void onResume() {
    super.onResume();
    registerReceiver(receiver, new IntentFilter(MonitoringService.LOG_UPDATE));
  }

  @Override
  protected void onPause() {
    super.onPause();
    unregisterReceiver(receiver);
  }

  @Override
  public boolean onCreateOptionsMenu(Menu menu) {
    // Inflate the menu; this adds items to the action bar if it is present.
    getMenuInflater().inflate(R.menu.manage, menu);
    return true;
  }

  @Override
  public boolean onOptionsItemSelected(MenuItem item) {
    switch (item.getItemId()) {
    case R.id.action_settings:
      startActivity(new Intent(this, ManageActivity.class));
      return true;
    default:
      return super.onOptionsItemSelected(item);
    }
  }
}
