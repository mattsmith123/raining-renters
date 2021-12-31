import {useEffect, useState} from 'react'
import Map from '../components/Map'
import EventStream from '../services/EventStream'
import dayjs from 'dayjs'
import _ from 'lodash'
import Head from 'next/head'


export const useInput = initialValue => {
    const [value, setValue] = useState(initialValue);
  
    return {
      value,
      setValue,
      reset: () => setValue(""),
      bind: {
        value,
        onChange: event => {
          setValue(event.target.value);
        }
      }
    };
  };
function ConfigInput({parentSetConfig}) {
    const { value:config, bind:bindConfig, reset:resetConfig } = useInput()
    const handleSubmit = (evt) => {
        evt.preventDefault()
        parentSetConfig(JSON.parse(config))
    }
    return <form onSubmit={handleSubmit}>
        <label>
            Config:
            <textarea name="config" {...bindConfig}></textarea>
        </label>
        <input type="submit" name="configSubmit"></input>
    </form>
}
function MapPage() {
  const [stream, setStream] = useState()
  const [data, setData] = useState([])
  const [timer, setTimer] = useState()
  const [config, setConfig] = useState()

  async function refreshPins() {
    window.stream = stream
    stream.read()
  }

  async function readLatest() {
    if (stream) {
        console.log('begin readlatest', stream.getLastTimestamp())
        data = await this.stream.readAsOf(stream.getLastTimestamp(), 5*60*1000)
        const ret = _.chain(data)
            .filter( u => u.searchInfo && !u.reservations)
            .map( u => ({lat: u.lat, lng: u.lng, id:u.consolidatedId}))
            .value()
        setData(ret)
    }
  }

  useEffect(async () => {
      if (config?.creds?.streamSettings) {
        const s = new EventStream(config.creds.streamSettings)
        console.log("stream created")
        s.prime(dayjs().subtract(config.startLag, config.startLagUnits).toDate())
        setStream(s)
      }
  }, [config])

  useEffect(() => {
    if (timer) {
        clearTimeout(timer)
        setTimer(null)
    }
    if (stream) {
        setTimer(setInterval(refreshPins, 1000))
        setInterval(readLatest, 500)
    }
  }, [stream])

  return <div>
      <Head>
        <script src="https://maps.googleapis.com/maps/api/js?sensorfalse"></script>
        <script src="https://code.jquery.com/jquery.min.js"></script>
        <script src="vendor/jquery.easing.1.3.js"></script>
        <script src="vendor/markerAnimate.js"></script>
        <script src="vendor/SlidingMarker.js"></script>
      </Head>
      {!config ? <ConfigInput parentSetConfig={setConfig}></ConfigInput> :
        (!data ?  <div>loading...</div> :
            <Map points={data} mapApiKey={config.creds.mapApiKey} center={config.center} zoom={config.zoom}></Map>
        )
      }
    </div>
}

export default MapPage