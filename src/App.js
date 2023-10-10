import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Typography, Autocomplete, TextField, Grid, Button, Stack, Chip } from '@mui/material';
import CircularProgress from '@mui/material/CircularProgress';

function App() {

    const [inputValue, setInputValue] = useState('');
    const [open, setOpen] = useState(false);
    const [options, setOptions] = useState([]);
    const [selectedMovies, setSelectedMovies] = useState([]);
    const [recommendedMovies, setRecommendedMovies] = useState([]);
    const [loading, setLoading] = useState(false);
    const [loadingRec, setLoadingRec] = useState(false);
    const [fetchingDone, setFetchingDone] = useState(false);

    useEffect(() => {
      let active = true;
      if (inputValue.length < 2) return;
      // use async IIFE cause useEffect cannot be async
      (async () => {
        let reqBody = { input: cleanTitle(inputValue) };
        setLoading(true);
        
        try {
          const response = await axios.post(
            'http://127.0.0.1:5000/search', 
            reqBody,
            {
              headers: {
                  'Content-Type': 'application/json',
              },
            }
          );
  
          let movies = response.data;
          if (active) {
            if (movies.length !== 0) setOptions(movies);
          }
        } catch (e) {
          console.log('error fetching options: ', e);
        } finally {
          setLoading(false);
          setFetchingDone(true);
        }
      })()

      // Run cleanup function runs when the component is unmounted or before running the effect again (due to dependency changes). 
      // Setting active to false to ensure if the component is unmounted before the async operation is complete, 
      // the setOptions function won't be called , thus preventing a potential memory leak
      return () => {
        active = false;
      }
    }, [inputValue])

    useEffect(() => {
      if (!open) {
        setOptions([]);
        setFetchingDone(false);
      }
    }, [open]);


    const cleanTitle = (title) => {
        return title.replace(/[^a-zA-Z0-9 ]/g, '');
    };

    const selectMovies = (event, newValue) => {
      if (!newValue) return;
      setSelectedMovies((prev) => {
        return [...prev, newValue]
      });
      setOptions([]);
      setInputValue('');
    };

    const deleteMovies = (i) => {
      const newMovies = selectedMovies.filter((m, index) => i !== index);
      setSelectedMovies(newMovies);
    };

    const getRecommendations = async () => {
      let reqBody = { input: selectedMovies };
      setLoadingRec(true);
      try {
        const response = await axios.post(
          'http://127.0.0.1:5000/recommend', 
          reqBody,
          {
            headers: {
                'Content-Type': 'application/json',
            },
          }
        );
        setRecommendedMovies(JSON.parse(response.data));
      } catch (e) {
        console.log('error fetching recommendations: ', e);
      } finally {
        setLoadingRec(false);
      }
    }

    return (
      <>
        <Typography textAlign='center' sx={{ fontWeight: 'bold', mt: 8 }}>
          Type in your favorite movies to get more recommendations!
        </Typography>
        <Grid container spacing={3} sx={{ display: 'flex', direction: 'column', justifyContent: 'center', alignItems:'center'}}>
          <Grid item xs={12} >
            <Autocomplete
              sx={{ width: '60%', m: 'auto' }}
              id="movie-search"
              open={open}
              inputValue={inputValue}
              onOpen={() => {
                setOpen(true);
              }}
              onClose={() => {
                setOpen(false);
              }}
              onInputChange={(event, newInputValue) => {
                setInputValue(newInputValue);
              }}
              onChange={selectMovies}
              getOptionLabel={(option) => option.clean_title}
              noOptionsText={  
                options.length === 0 
                ? (!fetchingDone ? "No results available" : "Loading...") 
                : null
              }
              options={options}
              loading={loading}
              renderInput={(params) => (
                  <TextField
                      {...params}
                      variant="standard"
                      label="Search for your favorite movies"
                      InputProps={{
                        ...params.InputProps,
                        endAdornment: (
                          <React.Fragment>
                            {loading ? <CircularProgress color="inherit" size={20} /> : null}
                            {params.InputProps.endAdornment}
                          </React.Fragment>
                        ),
                      }}
                  />
              )}
            />
          </Grid>

          {selectedMovies.length !== 0 &&
            <Grid item xs={12} container justifyContent="center">
              <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap'}}>
                {selectedMovies.map((m, i) => 
                  <Chip key={i} label={m.title} onDelete={() => deleteMovies(i)} />
                )}
              </Stack>
            </Grid>
          }


          <Grid item xs={12} container justifyContent="center">
            <Button variant="contained" onClick={() => getRecommendations()} >Get Recs</Button>
          </Grid>


          {loadingRec 
            ? (<Grid item xs={12} container justifyContent="center">
                <CircularProgress color="inherit" size={20} />
              </Grid>)
            : (recommendedMovies.length !== 0 &&
              (<Grid item xs={12} container justifyContent="center" sx={{ ml: 50, mr: 50}}>
                <Stack direction="row" gap={1} sx={{ flexWrap: 'wrap'}}>
                  {recommendedMovies.map((m, i) => 
                    <Chip key={i} label={m.title} color='success'/>
                  )}
                </Stack>
              </Grid>)
           )}

        </Grid>
      </>
  );
}

export default App;
