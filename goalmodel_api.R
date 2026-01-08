library(plumber)
library(jsonlite)

suppressWarnings({
  if (requireNamespace("goalmodel", quietly = TRUE)) {
    library(goalmodel)
  }
})

teams_data <- data.frame(
  team = c(
    "Inter",
    "Milan",
    "Napoli",
    "Juventus",
    "Roma",
    "Como",
    "Atalanta",
    "Bologna",
    "Lazio",
    "Udinese",
    "Cremonese",
    "Sassuolo",
    "Torino",
    "Parma",
    "Cagliari",
    "Lecce",
    "Genoa",
    "Verona",
    "Fiorentina",
    "Pisa"
  ),
  played = c(
    18,
    18,
    18,
    19,
    19,
    18,
    19,
    18,
    19,
    19,
    19,
    19,
    19,
    18,
    19,
    18,
    18,
    18,
    19,
    19
  ),
  gf = c(
    40,
    29,
    28,
    27,
    22,
    26,
    23,
    25,
    20,
    20,
    19,
    23,
    21,
    12,
    19,
    12,
    18,
    15,
    20,
    13
  ),
  ga = c(
    15,
    14,
    15,
    16,
    12,
    12,
    19,
    19,
    16,
    30,
    21,
    25,
    30,
    21,
    26,
    25,
    28,
    30,
    30,
    28
  ),
  stringsAsFactors = FALSE
)

get_team_row <- function(name) {
  idx <- which(teams_data$team == name)
  if (length(idx) == 0) {
    return(NULL)
  }
  teams_data[idx[1], ]
}

compute_lambdas <- function(home_team, away_team) {
  home_row <- get_team_row(home_team)
  away_row <- get_team_row(away_team)

  if (is.null(home_row) || is.null(away_row)) {
    avg_played <- mean(teams_data$played)
    avg_gf <- mean(teams_data$gf)
    avg_ga <- mean(teams_data$ga)
    if (is.null(home_row)) {
      home_row <- data.frame(
        played = avg_played,
        gf = avg_gf,
        ga = avg_ga
      )
    }
    if (is.null(away_row)) {
      away_row <- data.frame(
        played = avg_played,
        gf = avg_gf,
        ga = avg_ga
      )
    }
  }

  home_attack <- home_row$gf / max(home_row$played, 1)
  home_defence <- home_row$ga / max(home_row$played, 1)
  away_attack <- away_row$gf / max(away_row$played, 1)
  away_defence <- away_row$ga / max(away_row$played, 1)

  lambda_home <- max(0.2, (home_attack + away_defence) / 2 + 0.1)
  lambda_away <- max(0.2, (away_attack + home_defence) / 2 - 0.1)

  list(
    lambda_home = lambda_home,
    lambda_away = lambda_away
  )
}

fit_dc_model <- function() {
  if (!"goalmodel" %in% .packages(all.available = TRUE)) {
    return(NULL)
  }
  if (!file.exists("serie_a_results.csv")) {
    return(NULL)
  }
  matches <- tryCatch(
    read.csv("serie_a_results.csv", stringsAsFactors = FALSE),
    error = function(e) NULL
  )
  if (is.null(matches)) {
    return(NULL)
  }
  required_cols <- c("home", "away", "home_goals", "away_goals", "date")
  if (!all(required_cols %in% colnames(matches))) {
    return(NULL)
  }
  matches$date <- as.Date(matches$date)
  model <- tryCatch(
    goalmodel::fit_goalmodel(
      data = matches,
      home = "home",
      away = "away",
      home_goals = "home_goals",
      away_goals = "away_goals",
      model = "dc",
      time_weighting = TRUE
    ),
    error = function(e) NULL
  )
  model
}

dc_model <- fit_dc_model()

predict_match_dc <- function(model, home_team, away_team) {
  probs <- goalmodel::predict_result_probs(
    model,
    newdata = data.frame(
      home = home_team,
      away = away_team
    )
  )
  xg <- goalmodel::expected_goals(
    model,
    home_team,
    away_team
  )
  list(
    home_win_prob = probs$home * 100,
    draw_prob = probs$draw * 100,
    away_win_prob = probs$away * 100,
    expected_goals_home = xg$home,
    expected_goals_away = xg$away,
    expected_goals_total = xg$home + xg$away
  )
}

predict_match_poisson <- function(home_team, away_team) {
  lambdas <- compute_lambdas(home_team, away_team)
  lambda_home <- lambdas$lambda_home
  lambda_away <- lambdas$lambda_away

  max_goals <- 6
  goals <- 0:max_goals

  home_probs <- dpois(goals, lambda_home)
  away_probs <- dpois(goals, lambda_away)

  matrix_probs <- outer(home_probs, away_probs, "*")

  home_win_prob <- sum(matrix_probs[row(matrix_probs) > col(matrix_probs)]) * 100
  draw_prob <- sum(matrix_probs[row(matrix_probs) == col(matrix_probs)]) * 100
  away_win_prob <- sum(matrix_probs[row(matrix_probs) < col(matrix_probs)]) * 100

  btts_mask <- outer(goals > 0, goals > 0, "&")
  both_teams_to_score_prob <- sum(matrix_probs[btts_mask]) * 100

  total_goals_matrix <- outer(goals, goals, "+")
  over_25_mask <- total_goals_matrix >= 3
  over_25_prob <- sum(matrix_probs[over_25_mask]) * 100

  expected_goals_home <- lambda_home
  expected_goals_away <- lambda_away
  expected_goals_total <- expected_goals_home + expected_goals_away

  max_idx <- which(matrix_probs == max(matrix_probs), arr.ind = TRUE)[1, ]
  most_likely_scoreline <- paste0(goals[max_idx[1]], "-", goals[max_idx[2]])

  list(
    home_win_prob = round(home_win_prob, 1),
    draw_prob = round(draw_prob, 1),
    away_win_prob = round(away_win_prob, 1),
    expected_goals_home = round(expected_goals_home, 2),
    expected_goals_away = round(expected_goals_away, 2),
    expected_goals_total = round(expected_goals_total, 2),
    both_teams_to_score_prob = round(both_teams_to_score_prob, 1),
    over_25_prob = round(over_25_prob, 1),
    most_likely_scoreline = most_likely_scoreline
  )
}

predict_match <- function(home_team, away_team) {
  if (!is.null(dc_model)) {
    dc_res <- tryCatch(
      predict_match_dc(dc_model, home_team, away_team),
      error = function(e) NULL
    )
    if (!is.null(dc_res)) {
      home_win_prob <- dc_res$home_win_prob
      draw_prob <- dc_res$draw_prob
      away_win_prob <- dc_res$away_win_prob
      expected_goals_home <- dc_res$expected_goals_home
      expected_goals_away <- dc_res$expected_goals_away
      expected_goals_total <- dc_res$expected_goals_total
      goals <- 0:6
      home_probs <- dpois(goals, expected_goals_home)
      away_probs <- dpois(goals, expected_goals_away)
      matrix_probs <- outer(home_probs, away_probs, "*")
      btts_mask <- outer(goals > 0, goals > 0, "&")
      both_teams_to_score_prob <- sum(matrix_probs[btts_mask]) * 100
      total_goals_matrix <- outer(goals, goals, "+")
      over_25_mask <- total_goals_matrix >= 3
      over_25_prob <- sum(matrix_probs[over_25_mask]) * 100
      max_idx <- which(matrix_probs == max(matrix_probs), arr.ind = TRUE)[1, ]
      most_likely_scoreline <- paste0(goals[max_idx[1]], "-", goals[max_idx[2]])
      return(list(
        home_win_prob = round(home_win_prob, 1),
        draw_prob = round(draw_prob, 1),
        away_win_prob = round(away_win_prob, 1),
        expected_goals_home = round(expected_goals_home, 2),
        expected_goals_away = round(expected_goals_away, 2),
        expected_goals_total = round(expected_goals_total, 2),
        both_teams_to_score_prob = round(both_teams_to_score_prob, 1),
        over_25_prob = round(over_25_prob, 1),
        most_likely_scoreline = most_likely_scoreline
      ))
    }
  }
  predict_match_poisson(home_team, away_team)
}

pr <- pr()

pr <- pr %>%
  pr_post("/predict", function(req, res) {
    body <- tryCatch(
      fromJSON(req$postBody),
      error = function(e) NULL
    )

    if (is.null(body) || is.null(body$home_team) || is.null(body$away_team)) {
      res$status <- 400
      return(list(error = "Invalid request body"))
    }

    result <- predict_match(body$home_team, body$away_team)
    result
  })

pr$run(host = "0.0.0.0", port = 9001)
