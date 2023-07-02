import styled from "@emotion/styled";
import { Switch, alpha } from "@mui/material";
import { makeStyles } from "@mui/styles";

const useStyles = makeStyles({
    text: {
        fontFamily: 'Calibri',
        color: '#5C5958',
    },
    formControl: {
        '& .MuiOutlinedInput-root': {
          '&:hover fieldset': {
            borderColor: '#fff5b5',
          },
          '&.Mui-focused fieldset': {
            borderColor: '#fff5b5',
          },
        },
        '& .MuiInputLabel-root': {
            '&:hover': {
                color: '#5C5958',
              },
              '&.Mui-focused': {
                color: '#5C5958',
              },
        },
    },
    dayHeader: {
        fontFamily: 'sans-serif',
        color: '#5C5958',
    },

    calendar: {
        padding: '20px 0',
        height: '80vh',
        '& table': {
            borderRadius: 4,
        },
    },
});

const YellowSwitch = styled(Switch)(({ theme }) => ({
    '& .MuiSwitch-switchBase.Mui-checked': {
      color: '#FCE13D',
      '&:hover': {
        backgroundColor: alpha('#FCE13D', 0.2),
      },
    },
    '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
      backgroundColor: '#FCE13D',
    },
}));

export {YellowSwitch, useStyles}