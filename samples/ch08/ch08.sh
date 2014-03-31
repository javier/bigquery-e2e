# The commands can are for use with the material in Chapter 8
# BigQuery End to End.

echo 'Copy commands from this script into your terminal'
exit

# If you want to fetch the lastest version of the code from the
# repository you will need git installed before running this command.
GOOGLE_CODE_USER='username'
git clone https://${GOOGLE_CODE_USER}@code.google.com/p/bigquery-e2e/ 

# The chapter recommends installing the mobile app by using the
# download link at http://bigquery-sensors.appspot.com.
# However, if you have the Android SDK installed you can also use
# the adb tool to install the app over USB in debugging mode.
# See http://stackoverflow.com/a/16707217
adb -d install SensorsClient.apk

SERVICE_ACCOUNT='<account id>@developer.gserviceaccount.com'
DOWNLOADED_KEY='downloaded-privatekey.p12'
DEVAPPSERVER_KEY='/tmp/key-rsa.pem'
openssl pkcs12 -in ${DOWNLOADED_KEY} \
  -nodes -nocerts -passin pass:notasecret |
  openssl rsa -out ${DEVAPPSERVER_KEY}

# If the App Engine tools are on your path you can use
# bare tool name instead of the fully qualified paths.
# Otherwise, set this to your the SDK tooks directory.
APP_ENGINE_SDK='path to sdk'
# Command to run the local development server.
${APP_ENGINE_SDK}/dev_appserver.py \
  --appidentity_email_address=${SERVICE_ACCOUNT} \
  --appidentity_private_key_path=${DEVAPPSERVER_KEY} \
  sensors/cloud/src/app.yaml

# Command to upload sources to your private app instance.
# You must first edit app.yaml to reference your app id.
${APP_ENGINE_SDK}/appcfg.py update --oauth2 \
  sensors/cloud/src/app.yaml
