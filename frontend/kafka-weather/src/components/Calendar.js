import React, { useEffect, useState } from "react";

import { DateTime } from 'luxon';

import ptBR from '@fullcalendar/core/locales/pt-br';
import luxonPlugin from '@fullcalendar/luxon';
import FullCalendar from '@fullcalendar/react';
import timeGridPlugin from '@fullcalendar/timegrid';
import { useStyles } from "./style";

import './index.css'
import { Grid } from "@mui/material";

export default function Calendar({data}) {
    const [periods, setPeriods] = useState(undefined);

    const startOfThisWeek = DateTime.now().toISO();

    const classes = useStyles();

    useEffect(() => {
        if (data) {
            setPeriods(
            data.map((intervalo) => ({
              start: intervalo.inicio,
              end: intervalo.fim,
            }))
          );
        }
      }, [data]);

    return (
        // <Grid container spacing={4}>
            <Grid item xs={12} className={classes.calendar}>
                {periods && (
                    <FullCalendar
                        height='100%'
                        events={periods}
                        plugins={[timeGridPlugin, luxonPlugin]}
                        initialView="timeGridWeek"
                        dayHeaderFormat={{ weekday: 'short' }}
                        locale="pt-bR"
                        locales={[ptBR]}
                        headerToolbar={false}
                        allDaySlot={false}
                        nowIndicator={false}
                        now={startOfThisWeek}
                        slotLabelFormat="HH:mm"
                        slotDuration={'01:00:00'}
                        dayHeaderClassNames={classes.dayHeader}
                        slotLabelClassNames={classes.dayHeader}
                        eventColor="#106287"
                        eventBorderColor="none"
                        scrollTime="00:00:00"
                    />
                )}
            </Grid>
        // </Grid>
    );
}