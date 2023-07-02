import './App.css';
import { Grid } from '@mui/material';
import TopBar from './components/TopBar';
import { useEffect, useState } from 'react';
import Calendar from './components/Calendar';

import { DateTime } from 'luxon';

function App() {
  const [beachData, setBeachData] = useState(undefined)

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
      },
      {
        hora: '2023-07-01T01:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 0,
        local: 'Ubatuba',
      },
      {
        hora: '2023-07-01T02:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
      },
      {
        hora: '2023-07-01T03:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
      },
      {
        hora: '2023-07-02T04:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
      },
      {
        hora: '2023-07-02T05:00Z',
        temperatura: 20,
        pp: 0.8,
        uv: 1,
        boa_hora: 1,
        local: 'Ubatuba',
      }
    ]

    setBeachData(data.filter((item) => item.boa_hora).map((item) => ({...item, inicio: item.hora, fim: DateTime.fromISO(item.hora).plus({hours: 1}).toISO()})))
  }, [])

  return (
    <Grid style={{height: '100%', width: '100%', padding: '50px'}}>
      <TopBar />
      {beachData && <Calendar data={beachData} />}
    </Grid>
  );
}

export default App;
