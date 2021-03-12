const { Firestore } = require('@google-cloud/firestore');
const { PredictionServiceClient } = require('@google-cloud/aiplatform');
const PROJECTID = 'iotssc-project-303420';
const COLLECTION_NAME = 'iotssc';

const generateListValueEntry = (data) => {
  return {
    listValue: {
      values: [
        { numberValue: data[0] },
        { numberValue: data[1] },
        { numberValue: data[2] },
        { numberValue: data[3] },
        { numberValue: data[4] },
        { numberValue: data[5] },
      ]
    }
  }
}

const generateMlPredictionInstance = (data) => {
  const instance = {
    listValue: {
      values: []
    }
  }
  // TODO - currently we accept 128 recordings
  for (var i = 0; i < 128; i++) {
    instance.listValue.values.push(generateListValueEntry(data[i]));
  }
  return instance;
}

// Function calls ML endpoint to get classification prediction
const getMlPrediction = async (data) => {
  // Constants to define the endpoint we are targetting
  const endpointId = "5919453944597184512"
  const project = "iotssc-project-303420"
  const location = "europe-west4"
  // create the Service Client
  const clientOptions = {
    apiEndpoint: "europe-west4-prediction-aiplatform.googleapis.com"
  }
  const client = new PredictionServiceClient(clientOptions);

  // TODO -- generate the correct instance from the data
  const instance = generateMlPredictionInstance(data);

  const clientRequestOptions = {
    endpoint: `projects/${project}/locations/${location}/endpoints/${endpointId}`,
    instances: [instance],
  }

  const [response] = await client.predict(clientRequestOptions);

  return response;
}

const parseIoTData = (rawData) => {
  console.log("Parsing IoT Data");
  const bufferLength = 128;
  const elementLength = 18;
  const shortLength = 2;
  const floatLength = 4;
  const buffer = Buffer.from(rawData, 'base64');
  let recording = new Array(bufferLength);

  for (let i = 0; i < buffer.length; i += elementLength) {
    // accel
    const x_accel = buffer.readIntLE(i, shortLength) || 0.0;
    const y_accel = buffer.readIntLE(i + 2, shortLength) || 0.0;
    const z_accel = buffer.readIntLE(i + 4, shortLength) || 0.0;

    // gyro
    const x_gyro = buffer.readFloatLE(i + 6, floatLength) || 0.0;
    const y_gyro = buffer.readFloatLE(i + 10, floatLength) || 0.0;
    const z_gyro = buffer.readFloatLE(i + 14, floatLength) || 0.0;

    recording[i / elementLength] = new Array(6);
    recording[i / elementLength] = [x_accel, y_accel, z_accel, x_gyro, y_gyro, z_gyro];
  }

  // console.log(recording);

  return recording;
}

const mainHandler = async (req, res) => {

  // extract the relevant parts from the request
  const rawSensorData = req.body.notifications[0].payload;
  const resourceId = req.body.notifications[0].path

  // parse the raw data into a 2d array
  const parsedSensorData = parseIoTData(rawSensorData);
  // labels for the activity classifcation
  const labels = ["WALKING", "WALKING_UPSTAIRS", "WALKING_DOWNSTAIRS", "SITTING", "STANDING", "LAYING"];
  // call the predition endpoint to get a prediction
  const mlPrediction = await getMlPrediction(parsedSensorData);
  // parse prediction for DB storage
  const activityPrediciton = mlPrediction.predictions[0].listValue.values.map((elem, i) => {
    const value = elem.numberValue;
    const label = labels[i];
    return {
      value,
      label
    };
  });
  // console.log(activityPrediciton);

  // parse the sensor data for db storage
  const sensorData = parsedSensorData.map((rec) => {
    return {
      accel_x: rec[0],
      accel_y: rec[1],
      accel_z: rec[2],
      gyro_x: rec[3],
      gyro_y: rec[4],
      gyro_z: rec[5],
    }
  })


  // Upload to firebase
  const created = new Date().getTime();

  const firestore = new Firestore();
  await firestore.collection(COLLECTION_NAME)
    .add({ created, resourceId, activityPrediciton, sensorData });

  let message = "All actions completed successfully";
  res.status(200).send(message);
}

exports.main = (req, res) => {
  // always reply 200 so the sever keeps sending data
  mainHandler(req, res)
    .then(() => { res.status(200).send() })
    .catch((e) => {
      res.status(200).send(JSON.stringify(e));
      console.log(e);
    });
};
