import './App.css';
import { Grid } from '@mui/material';
import TopBar from './components/TopBar';
import { useEffect, useState } from 'react';
import Calendar from './components/Calendar';

import { DateTime } from 'luxon';

function App() {
  const [fullData, setFullData] = useState(undefined)
  const [beachData, setBeachData] = useState(undefined)
  const [checkedIsDay, setCheckedIsDay] = useState(false);
  const [city, setCity] = useState('')
  const [byPeriod, setByPeriod] = useState(true)

  useEffect(() => {
    // TODO: pegar da api

    const data = [
      {
        hora: '2023-07-01T00:00Z',
        temperatura: 20,  
        pp: 0.8,
        uv: 1,
        boa_hora: 0,
        local: 'Ubatuba',
        isDay: 0,
      },
      {
        hora: '2023-07-01T01:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 0,
        local: 'Ubatuba',
        isDay: 0,
      },
      {
        hora: '2023-07-01T02:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
        isDay: 1,
      },
      {
        hora: '2023-07-01T03:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
        isDay: 0,
      },
      {
        hora: '2023-07-02T04:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
        isDay: 1,
      },
      {
        hora: '2023-07-02T05:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
        isDay: 0,
      }
    ]

    setFullData(data)

    if(!byPeriod) {
      setBeachData(data.filter((item) => item.boa_hora).map((item) => ({...item, inicio: item.hora, fim: DateTime.fromISO(item.hora).plus({hours: 1}).toISO()})))
    } else {
      setBeachData(data.filter((item) => item.boa_hora))
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
      {beachData && <Calendar data={beachData} />}
    </Grid>
  );
}

export default App;
