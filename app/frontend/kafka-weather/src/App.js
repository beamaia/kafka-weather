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
    console.log('fullData', fullData);
    }, [fullData])

  useEffect(() => {
    const hourDay = byPeriod ? 'day' : 'hour';
    
    if (city) {
      setIsLoading(true)

      api.get(`/beach_${hourDay}/?city=${city}`).then((response) => {
        const data = response.data;

        setFullData(data.filter((item) => item.boaHora))
        
        setIsLoading(false)
      })
      .catch((reason) => {
        setIsLoading(false)
        console.error(reason);
      });
    }

  }, [byPeriod, city])


  useEffect(() => {
    if (checkedIsDay && fullData) {
      const dayData = fullData.filter((item) =>  item.isDay)
      if(!byPeriod) {
        setBeachData(dayData.filter((item) => item.boaHora).map((item) => ({...item, inicio: item.hora, fim: DateTime.fromISO(item.hora).plus({hours: 1}).toISO()})))
      } else {
        setBeachData(dayData.filter((item) => item.boaHora))
      }
    } else if (!checkedIsDay && fullData) {
      if(!byPeriod) {
        setBeachData(fullData.filter((item) => item.boaHora).map((item) => ({...item, inicio: item.hora, fim: DateTime.fromISO(item.hora).plus({hours: 1}).toISO()})))
      } else {
        setBeachData(fullData.filter((item) => item.boaHora))
      }
    }
  }, [checkedIsDay, fullData, byPeriod])

  return (
    <Grid style={{height: '100vh', width: '100%', padding: '50px'}}>
      <TopBar isDayState={[checkedIsDay, setCheckedIsDay]} cityState={[city, setCity]} byPeriodState={[byPeriod, setByPeriod]} />
      {isLoading ? 
        <Grid container spacing={2} style={{display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%'}}>
          <CircularProgress  />
        </Grid>
      : 
        beachData && <Calendar data={beachData} />
      }
    </Grid>
  );
}

export default App;
