import { Wrapper, Status } from "@googlemaps/react-wrapper";
import React from 'react'
import { createCustomEqual } from "fast-equals";
//import mao from 'marker-animate-unobtrusive'

function Point ({map, lat, lng, state, created, expires}) {
    const [marker, setMarker] = React.useState();

    React.useEffect(() => {
      if (!marker) {
        const m = new SlidingMarker({ duration: 2000, easing: 'easeOutExpo'})
        setMarker(m)
      }

      // remove marker from map on unmount
      return () => {
        if (marker) {
          marker.setMap(null);
        }
      };
    }, [marker]);

    React.useEffect(() => {
      if (marker) {
        marker.setOptions({
            map, 
            position: {lat, lng},
            animation: google.maps.Animation.DROP
        });
      }
    }, [marker, lat, lng]);
    return null;
}



function Map ({points, center, zoom}) {
    const ref = React.useRef(null);
    const [map, setMap] = React.useState();
    React.useEffect(() => {
        if (ref.current && !map) {
            setMap(new window.google.maps.Map(ref.current, {center, zoom}));
        }
    }, [ref, map]);

    return (
        <div className="map">
        <div style = {{ display: "flex", height: "100%" }} ref={ref}>
            {
              points.map( (point) => <Point map={map} {...point} key={point.id}></Point>)
            }
        </div>
        </div>
    )
}

const render = (status) => {
    return <h1>{status}</h1>;
};

export default  function({mapApiKey, ...props})  {
    return (
        <Wrapper apiKey={mapApiKey} render={render}>
            <Map mapApiKey={mapApiKey} {...props}/>
        </Wrapper>
    )
}