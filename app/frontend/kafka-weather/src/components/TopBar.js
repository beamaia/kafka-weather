import { Checkbox, FormControl, FormControlLabel, Grid, InputLabel, MenuItem, Select } from "@mui/material";
import React from "react";
import { YellowSwitch, useStyles } from "./style";
import { citiesEnum } from "../assets/cities";

export default function TopBar({isDayState, cityState, byPeriodState}) {
    const [checkedIsDay, setCheckedIsDay] = isDayState;
    const [city, setCity] = cityState;
    const [byPeriod, setByPeriod] = byPeriodState;
    const classes = useStyles();
    const keys = Object.keys(citiesEnum);

    return (
    <Grid container spacing={2} style={{display: 'flex', }}>
        <Grid item xs={6} style={{padding: '16px 20px 0'}}>
            <FormControl 
                sx={{  width: '100%' }} 
                size="small"
                className={classes.formControl}
            >
            <InputLabel id="select-cidade">Cidade</InputLabel>
            <Select
                labelId="select-cidade"
                value={city}
                onChange={(e) => setCity(e.target.value)}
                label="Cidade"
                // change select box color to yellow
                sx={{
                    '& .MuiSelect-icon': {
                        color: '#FCE13D',
                    },
                }}
                className={classes.text}
            >
                {keys.map((city, index) => (
                    <MenuItem className={classes.text} key={index} value={citiesEnum[city]}>{citiesEnum[city]}</MenuItem>
                ))}
            </Select>
            </FormControl>
        </Grid>
        <Grid item xs={3} style={{display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
            <FormControlLabel
                label="Durante dia"
                className={classes.text}
                control={
                    <Checkbox   
                        sx={{
                            color: '#f7d819',
                            '&.Mui-checked': {
                                color: '#FCE13D',
                            },
                        }} 
                        checked={checkedIsDay} 
                        onChange={() => setCheckedIsDay((prev) => !prev)} 
                    />
                }
            />
        </Grid>
        <Grid item xs={3} style={{display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
            <FormControlLabel className={classes.text} control={<YellowSwitch checked={byPeriod} onChange={() => setByPeriod((prev) => !prev)} />} label="Mostrar por período" />
        </Grid>
    </Grid>
    );
}