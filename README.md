# Streamed Cache

Given an API which returns the current temperature that is measured in different cities, exposing two ways to query it:
- A slow and expensive fetch call, which returns the current temperatures for all cities that are being tracked.
- A faster and incremental subscribe method, which only returns updates when the temperature in a city changes.

This build a performant and robust cache on top of this API, which allows consumers to get the current temperature for any tracked city without an asynchronous network call, by keeping the a map of cities and their current temperature up to date in the background.

The goal of the cache is to always return the most recent value the API has delivered.

To check the solution run cargo test.
