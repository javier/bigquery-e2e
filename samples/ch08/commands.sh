# The commands can are for use with the material in Chapter 8
# BigQuery End to End.

# Fetch the latest sample code from the googlecode.com repo.
git clone https://siddartha.naidu@code.google.com/p/bigquery-e2e/ 

# Command to load the APK using ADB
adb -d install SensorsClient.apk

# Command to run the local development server.
${APP_ENGINE_SDK}/dev_appserver.py \
  --appidentity_email_address <account id>@developer.gserviceaccount.com \
  --appidentity_private_key_path /tmp/privatekey-rsa.pem \
  sensors/cloud/src/app.yaml

