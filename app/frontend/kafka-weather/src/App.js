import './App.css';
import { CircularProgress, Grid } from '@mui/material';
import TopBar from './components/TopBar';
import { useEffect, useState } from 'react';
import Calendar from './components/Calendar';

import { DateTime } from 'luxon';
import api from './api';

function App() {
  const [fullData, setFullData] = useState(undefined)
  const [isLoading, setIsLoading] = useState(false)
  const [beachData, setBeachData] = useState(undefined)
  const [checkedIsDay, setCheckedIsDay] = useState(false);
  const [city, setCity] = useState(undefined)
  const [byPeriod, setByPeriod] = useState(true)


  useEffect(() => {
    // TODO: pegar da api
    setIsLoading(true)
    const hourDay = byPeriod ? 'day' : 'hour';

    if (city) {
      api.get(`/beach_${hourDay}/?city=${city}`).then((response) => {
        const { data } = response.data;
        
        setFullData(data)

        if(!byPeriod) {
          setBeachData(data.filter((item) => item.boaHora).map((item) => ({...item, inicio: item.hora, fim: DateTime.fromISO(item.hora).plus({hours: 1}).toISO()})))
        } else {
          setBeachData(data.filter((item) => item.boaHora))
        }
  
        setIsLoading(false)
      })
      .catch((reason) => {
        console.error(reason);
      });
    }

  }, [byPeriod, city])

  useEffect(() => {
    if (checkedIsDay && fullData) {
      setBeachData(fullData.filter((item) =>  item.isDay))
    }
  }, [checkedIsDay, fullData])

  return (
    <Grid style={{height: '100%', width: '100%', padding: '50px'}}>
      <TopBar isDayState={[checkedIsDay, setCheckedIsDay]} cityState={[city, setCity]} byPeriodState={[byPeriod, setByPeriod]} />
      {isLoading && <CircularProgress color="success" />}
      {beachData && <Calendar data={beachData} />}
    </Grid>
  );
}

export default App;
