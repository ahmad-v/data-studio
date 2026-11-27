use localwright_core::{Error, Job, Result};
use serde_json::{json, Value};

const BOOTSTRAP_RESAMPLES: usize = 2_000;
const MAX_BOOTSTRAP_N: usize = 5_000;
const Z_975: f64 = 1.959_963_984_540_054;

fn require_minimum(values: &[f64], minimum: usize) -> Result<()> {
    if values.len() < minimum {
        Err(Error::input(format!(
            "This analysis requires at least {minimum} complete observations"
        )))
    } else {
        Ok(())
    }
}

fn mean_variance(values: &[f64]) -> Result<(f64, f64)> {
    require_minimum(values, 2)?;
    let mut mean = 0.0;
    let mut m2 = 0.0;
    for (index, value) in values.iter().enumerate() {
        let n = (index + 1) as f64;
        let delta = value - mean;
        mean += delta / n;
        m2 += delta * (value - mean);
    }
    if !mean.is_finite() || !m2.is_finite() {
        return Err(Error::input("Numerical overflow in statistics"));
    }
    Ok((mean, (m2 / (values.len() - 1) as f64).max(0.0)))
}

fn quantile_sorted(sorted: &[f64], probability: f64) -> Result<f64> {
    if sorted.is_empty() || !(0.0..=1.0).contains(&probability) {
        return Err(Error::input(
            "Quantiles require values and probability in 0..=1",
        ));
    }
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let fraction = position - lower as f64;
    Ok(sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction)
}

pub(crate) fn quantiles(values: &[f64]) -> Result<Value> {
    require_minimum(values, 1)?;
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let q1 = quantile_sorted(&sorted, 0.25)?;
    let median = quantile_sorted(&sorted, 0.5)?;
    let q3 = quantile_sorted(&sorted, 0.75)?;
    Ok(json!({
        "n": sorted.len(), "q1": q1, "median": median, "q3": q3,
        "iqr": q3-q1, "method": "Hyndman-Fan type 7 linear interpolation"
    }))
}

fn log_gamma(z: f64) -> f64 {
    const COEFFICIENTS: [f64; 8] = [
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if z < 0.5 {
        return std::f64::consts::PI.ln()
            - (std::f64::consts::PI * z).sin().ln()
            - log_gamma(1.0 - z);
    }
    let shifted = z - 1.0;
    let mut x = 0.999_999_999_999_809_9;
    for (index, coefficient) in COEFFICIENTS.iter().enumerate() {
        x += coefficient / (shifted + index as f64 + 1.0);
    }
    let t = shifted + 7.5;
    0.5 * (2.0 * std::f64::consts::PI).ln() + (shifted + 0.5) * t.ln() - t + x.ln()
}

fn beta_fraction(a: f64, b: f64, x: f64) -> f64 {
    const MAX_ITER: usize = 200;
    const EPSILON: f64 = 3.0e-14;
    const FLOOR: f64 = 1.0e-300;
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < FLOOR {
        d = FLOOR;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..=MAX_ITER {
        let m = m as f64;
        let m2 = 2.0 * m;
        let mut aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < FLOOR {
            d = FLOOR;
        }
        c = 1.0 + aa / c;
        if c.abs() < FLOOR {
            c = FLOOR;
        }
        d = 1.0 / d;
        h *= d * c;
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < FLOOR {
            d = FLOOR;
        }
        c = 1.0 + aa / c;
        if c.abs() < FLOOR {
            c = FLOOR;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() <= EPSILON {
            break;
        }
    }
    h
}

fn regularized_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let exponent = log_gamma(a + b) - log_gamma(a) - log_gamma(b) + a * x.ln() + b * (-x).ln_1p();
    let factor = exponent.exp();
    let value = if x < (a + 1.0) / (a + b + 2.0) {
        factor * beta_fraction(a, b, x) / a
    } else {
        1.0 - factor * beta_fraction(b, a, 1.0 - x) / b
    };
    value.clamp(0.0, 1.0)
}

fn student_t_two_sided_p(statistic: f64, degrees_freedom: f64) -> Result<f64> {
    if !degrees_freedom.is_finite() || degrees_freedom <= 0.0 || statistic.is_nan() {
        return Err(Error::input(
            "Invalid Student-t statistic or degrees of freedom",
        ));
    }
    if statistic.is_infinite() {
        return Ok(0.0);
    }
    let t2 = statistic * statistic;
    let x = if t2.is_infinite() {
        0.0
    } else {
        degrees_freedom / (degrees_freedom + t2)
    };
    Ok(regularized_beta(x, degrees_freedom / 2.0, 0.5))
}

fn student_t_critical_975(degrees_freedom: f64) -> Result<f64> {
    if !degrees_freedom.is_finite() || degrees_freedom <= 0.0 {
        return Err(Error::input("Invalid degrees of freedom"));
    }
    let mut low = 0.0;
    let mut high = 1.0;
    while student_t_two_sided_p(high, degrees_freedom)? > 0.05 {
        high *= 2.0;
        if high > 1e8 {
            return Err(Error::input("Unable to calculate t critical value"));
        }
    }
    for _ in 0..64 {
        let middle = (low + high) / 2.0;
        if student_t_two_sided_p(middle, degrees_freedom)? > 0.05 {
            low = middle;
        } else {
            high = middle;
        }
    }
    Ok((low + high) / 2.0)
}

pub(crate) fn standard_error(values: &[f64]) -> Result<Value> {
    let (mean, variance) = mean_variance(values)?;
    let se = (variance / values.len() as f64).sqrt();
    Ok(
        json!({"n":values.len(), "mean":mean, "sample_sd":variance.sqrt(), "standard_error":se,
        "method":"sample standard deviation divided by square root of n"}),
    )
}

pub(crate) fn mean_confidence_interval(values: &[f64]) -> Result<Value> {
    let (mean, variance) = mean_variance(values)?;
    let df = (values.len() - 1) as f64;
    let se = (variance / values.len() as f64).sqrt();
    let critical = student_t_critical_975(df)?;
    let margin = critical * se;
    let (lower, upper) = (mean - margin, mean + margin);
    if !se.is_finite() || !margin.is_finite() || !lower.is_finite() || !upper.is_finite() {
        return Err(Error::input(
            "Numerical overflow in mean confidence interval",
        ));
    }
    Ok(json!({"n":values.len(), "mean":mean, "standard_error":se,
        "confidence_level":0.95, "degrees_freedom":df, "critical_t":critical,
        "lower":lower, "upper":upper,
        "method":"two-sided Student-t interval, sample variance, independent observations"}))
}

fn rng_next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn bootstrap(values: &[f64], median: bool, job: &Job) -> Result<Value> {
    require_minimum(values, 2)?;
    if values.len() > MAX_BOOTSTRAP_N {
        return Err(Error::new(
            localwright_core::ErrorKind::Limit,
            format!("Bootstrap is limited to {MAX_BOOTSTRAP_N} observations"),
        ));
    }
    let mut seed = 0xcbf2_9ce4_8422_2325u64;
    for value in values {
        seed ^= value.to_bits();
        seed = seed.wrapping_mul(0x100_0000_01b3);
    }
    if seed == 0 {
        seed = 0x9e37_79b9_7f4a_7c15;
    }
    let statistic = |sample: &mut Vec<f64>| {
        if median {
            sample.sort_by(f64::total_cmp);
            quantile_sorted(sample, 0.5).expect("nonempty resample")
        } else {
            sample.iter().sum::<f64>() / sample.len() as f64
        }
    };
    let initial_seed = seed;
    let mut state = seed;
    let mut sample = Vec::with_capacity(values.len());
    let mut estimates = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for iteration in 0..BOOTSTRAP_RESAMPLES {
        if iteration % 32 == 0 {
            job.check()?;
        }
        sample.clear();
        for _ in values {
            sample.push(values[(rng_next(&mut state) % values.len() as u64) as usize]);
        }
        estimates.push(if median {
            statistic(&mut sample)
        } else {
            mean_variance(&sample)?.0
        });
    }
    estimates.sort_by(f64::total_cmp);
    let lower = quantile_sorted(&estimates, 0.025)?;
    let upper = quantile_sorted(&estimates, 0.975)?;
    let observed = if median {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        quantile_sorted(&sorted, 0.5)?
    } else {
        mean_variance(values)?.0
    };
    Ok(
        json!({"n":values.len(), "estimate":observed, "confidence_level":0.95,
        "lower":lower, "upper":upper, "resamples":BOOTSTRAP_RESAMPLES,
        "seed":format!("fnv1a64-xorshift64:0x{initial_seed:016x}"),
        "method":"deterministic percentile bootstrap; not bias-corrected"}),
    )
}

pub(crate) fn bootstrap_mean_interval(values: &[f64], job: &Job) -> Result<Value> {
    bootstrap(values, false, job)
}

pub(crate) fn bootstrap_median_interval(values: &[f64], job: &Job) -> Result<Value> {
    bootstrap(values, true, job)
}

fn sample_variance(values: &[f64]) -> Result<(f64, f64)> {
    mean_variance(values)
}

pub(crate) fn welch_t_test(first: &[f64], second: &[f64]) -> Result<Value> {
    let (mean_a, variance_a) = sample_variance(first)?;
    let (mean_b, variance_b) = sample_variance(second)?;
    let component_a = variance_a / first.len() as f64;
    let component_b = variance_b / second.len() as f64;
    let standard_error = (component_a + component_b).sqrt();
    if standard_error == 0.0 {
        return Err(Error::input(
            "Welch test requires nonzero within-sample variance",
        ));
    }
    let df = (component_a + component_b).powi(2)
        / (component_a.powi(2) / (first.len() - 1) as f64
            + component_b.powi(2) / (second.len() - 1) as f64);
    let difference = mean_a - mean_b;
    let statistic = difference / standard_error;
    if !difference.is_finite() || !standard_error.is_finite() || !statistic.is_finite() {
        return Err(Error::input("Numerical overflow in Welch test"));
    }
    let p = student_t_two_sided_p(statistic, df)?;
    let margin = student_t_critical_975(df)? * standard_error;
    let (lower, upper) = (difference - margin, difference + margin);
    if !df.is_finite() || !margin.is_finite() || !lower.is_finite() || !upper.is_finite() {
        return Err(Error::input("Numerical overflow in Welch test"));
    }
    Ok(
        json!({"n_first":first.len(), "n_second":second.len(), "mean_first":mean_a,
        "mean_second":mean_b, "difference_first_minus_second":difference,
        "standard_error":standard_error, "t_statistic":statistic, "degrees_freedom":df,
        "p_value_two_sided":p, "confidence_level":0.95,
        "difference_ci_lower":lower, "difference_ci_upper":upper,
        "method":"Welch-Satterthwaite two-sample Student-t test; unequal variances"}),
    )
}

pub(crate) fn paired_t_test(first: &[f64], second: &[f64]) -> Result<Value> {
    if first.len() != second.len() {
        return Err(Error::input("Paired samples have different lengths"));
    }
    let differences = first
        .iter()
        .zip(second)
        .map(|(a, b)| a - b)
        .collect::<Vec<_>>();
    if differences.iter().any(|difference| !difference.is_finite()) {
        return Err(Error::input("Numerical overflow in paired differences"));
    }
    let (mean, variance) = mean_variance(&differences)?;
    let se = (variance / differences.len() as f64).sqrt();
    if se == 0.0 {
        return Err(Error::input(
            "Paired test requires nonzero difference variance",
        ));
    }
    let df = (differences.len() - 1) as f64;
    let statistic = mean / se;
    let margin = student_t_critical_975(df)? * se;
    let (lower, upper) = (mean - margin, mean + margin);
    if !se.is_finite()
        || !statistic.is_finite()
        || !margin.is_finite()
        || !lower.is_finite()
        || !upper.is_finite()
    {
        return Err(Error::input("Numerical overflow in paired test"));
    }
    Ok(
        json!({"n_pairs":differences.len(), "mean_difference_first_minus_second":mean,
        "standard_error":se, "t_statistic":statistic, "degrees_freedom":df,
        "p_value_two_sided":student_t_two_sided_p(statistic,df)?, "confidence_level":0.95,
        "difference_ci_lower":lower, "difference_ci_upper":upper,
        "method":"paired Student-t test on complete differences"}),
    )
}

#[derive(Debug)]
pub(crate) struct GroupSample {
    pub label: String,
    pub values: Vec<f64>,
}

pub(crate) fn one_way_anova(groups: &[GroupSample]) -> Result<Value> {
    if groups.len() < 2 || groups.iter().any(|group| group.values.is_empty()) {
        return Err(Error::input("ANOVA requires at least two nonempty groups"));
    }
    let total = groups.iter().map(|group| group.values.len()).sum::<usize>();
    if total <= groups.len() {
        return Err(Error::input(
            "ANOVA requires positive within-group degrees of freedom",
        ));
    }
    let mut grand_sum = 0.0;
    let mut within_ss = 0.0;
    let mut group_summaries = Vec::with_capacity(groups.len());
    for group in groups {
        let (mean, _) = mean_variance(&group.values).or_else(|error| {
            if group.values.len() == 1 {
                Ok((group.values[0], 0.0))
            } else {
                Err(error)
            }
        })?;
        grand_sum += mean * group.values.len() as f64;
        within_ss += group
            .values
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>();
        group_summaries.push(json!({"group":group.label,"n":group.values.len(),"mean":mean}));
    }
    let grand_mean = grand_sum / total as f64;
    let between_ss = groups
        .iter()
        .zip(&group_summaries)
        .map(|(group, summary)| {
            let mean = summary["mean"].as_f64().expect("mean written above");
            group.values.len() as f64 * (mean - grand_mean).powi(2)
        })
        .sum::<f64>();
    let df_between = (groups.len() - 1) as f64;
    let df_within = (total - groups.len()) as f64;
    if !within_ss.is_finite() || !between_ss.is_finite() || within_ss <= 0.0 {
        return Err(Error::input(
            "ANOVA requires positive finite within-group variance",
        ));
    }
    let f_statistic = (between_ss / df_between) / (within_ss / df_within);
    if !f_statistic.is_finite() || f_statistic < 0.0 {
        return Err(Error::input("Numerical overflow in ANOVA statistic"));
    }
    let x = df_within / (df_within + df_between * f_statistic);
    let p = regularized_beta(x, df_within / 2.0, df_between / 2.0);
    Ok(
        json!({"groups":group_summaries,"n":total,"grand_mean":grand_mean,
        "f_statistic":f_statistic,"df_between":df_between,"df_within":df_within,
        "p_value":p,"method":"one-way fixed-effects ANOVA; independent groups, finite values"}),
    )
}

fn ranks(values: &[f64]) -> Vec<f64> {
    let mut order = (0..values.len()).collect::<Vec<_>>();
    order.sort_by(|a, b| values[*a].total_cmp(&values[*b]));
    let mut result = vec![0.0; values.len()];
    let mut start = 0;
    while start < order.len() {
        let mut end = start + 1;
        while end < order.len() && values[order[start]] == values[order[end]] {
            end += 1;
        }
        let average_rank = ((start + 1 + end) as f64) / 2.0;
        for index in start..end {
            result[order[index]] = average_rank;
        }
        start = end;
    }
    result
}

fn pearson_values(x: &[f64], y: &[f64]) -> Result<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return Err(Error::input(
            "Correlation requires equal samples of at least two pairs",
        ));
    }
    let (mean_x, _) = mean_variance(x)?;
    let (mean_y, _) = mean_variance(y)?;
    let mut xx = 0.0;
    let mut yy = 0.0;
    let mut xy = 0.0;
    for (a, b) in x.iter().zip(y) {
        xx += (a - mean_x).powi(2);
        yy += (b - mean_y).powi(2);
        xy += (a - mean_x) * (b - mean_y);
    }
    if xx <= 0.0 || yy <= 0.0 {
        return Err(Error::input(
            "Correlation requires nonzero variance in both samples",
        ));
    }
    Ok((xy / xx.sqrt() / yy.sqrt()).clamp(-1.0, 1.0))
}

pub(crate) fn spearman(x: &[f64], y: &[f64]) -> Result<Value> {
    if x.len() != y.len() || x.len() < 3 {
        return Err(Error::input(
            "Spearman analysis requires at least three complete pairs",
        ));
    }
    let rho = pearson_values(&ranks(x), &ranks(y))?;
    let df = (x.len() - 2) as f64;
    let t = if rho.abs() >= 1.0 {
        f64::INFINITY
    } else {
        rho * (df / (1.0 - rho * rho)).sqrt()
    };
    Ok(
        json!({"n":x.len(), "rho":rho, "t_approx_statistic":if t.is_finite(){Some(t)}else{None},
        "degrees_freedom_approximation":df,"p_value_two_sided_approximation":student_t_two_sided_p(t,df)?,
        "method":"Spearman rank correlation with average ranks for ties; t approximation for p-value"}),
    )
}

pub(crate) fn pearson_interval(r: f64, n: usize) -> Result<Value> {
    if n <= 3 || !r.is_finite() || r.abs() >= 1.0 {
        return Err(Error::input(
            "Fisher-z correlation interval requires n > 3 and |r| < 1",
        ));
    }
    let z = r.atanh();
    let margin = Z_975 / ((n - 3) as f64).sqrt();
    let lower = (z - margin).tanh();
    let upper = (z + margin).tanh();
    if !lower.is_finite() || !upper.is_finite() {
        return Err(Error::input("Numerical overflow in correlation interval"));
    }
    Ok(
        json!({"n":n,"r":r,"confidence_level":0.95,"lower":lower,"upper":upper,
        "method":"Fisher-z asymptotic interval; assumes independent bivariate-normal pairs"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_use_type_seven_interpolation() {
        let report = quantiles(&[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
        assert_eq!(report["q1"], 2.0);
        assert_eq!(report["median"], 3.0);
        assert_eq!(report["q3"], 4.0);
        assert_eq!(report["iqr"], 2.0);
    }

    #[test]
    fn student_t_probability_and_critical_values_match_reference() {
        let p = student_t_two_sided_p(2.0, 10.0).unwrap();
        assert!((p - 0.073_388_034_770_740_4).abs() < 1e-11);
        let critical = student_t_critical_975(10.0).unwrap();
        assert!((critical - 2.228_138_851_964_938_5).abs() < 1e-10);
        assert!((student_t_critical_975(1.0).unwrap() - 12.706_204_736_432_1).abs() < 1e-8);
    }

    #[test]
    fn confidence_interval_includes_mean_and_has_expected_width() {
        let report = mean_confidence_interval(&[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(report["mean"], 2.0);
        assert!((report["lower"].as_f64().unwrap() - (-0.484_137_711_7)).abs() < 1e-8);
        assert!((report["upper"].as_f64().unwrap() - 4.484_137_711_7).abs() < 1e-8);
    }

    #[test]
    fn standard_error_matches_sample_standard_deviation_reference() {
        let report = standard_error(&[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(report["n"], 3);
        assert_eq!(report["mean"], 2.0);
        assert_eq!(report["sample_sd"], 1.0);
        assert!((report["standard_error"].as_f64().unwrap() - 1.0 / 3.0_f64.sqrt()).abs() < 1e-14);
    }

    #[test]
    fn welch_test_matches_reference_for_equal_variance_samples() {
        let report = welch_t_test(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]).unwrap();
        assert!((report["t_statistic"].as_f64().unwrap() + 3.674_234_614_174_767).abs() < 1e-10);
        assert_eq!(report["degrees_freedom"], 4.0);
        assert!(
            (report["p_value_two_sided"].as_f64().unwrap() - 0.021_311_641_128_756_7).abs() < 1e-9
        );
    }

    #[test]
    fn paired_test_uses_complete_difference_vector() {
        let report = paired_t_test(&[2.0, 4.0, 6.0, 8.0], &[1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(report["n_pairs"], 4);
        assert_eq!(report["mean_difference_first_minus_second"], 2.5);
        assert!(report["p_value_two_sided"].as_f64().unwrap() < 0.05);
    }

    #[test]
    fn anova_reports_expected_between_and_within_degrees() {
        let groups = vec![
            GroupSample {
                label: "a".into(),
                values: vec![1.0, 2.0, 3.0],
            },
            GroupSample {
                label: "b".into(),
                values: vec![2.0, 3.0, 4.0],
            },
            GroupSample {
                label: "c".into(),
                values: vec![5.0, 6.0, 7.0],
            },
        ];
        let report = one_way_anova(&groups).unwrap();
        assert_eq!(report["df_between"], 2.0);
        assert_eq!(report["df_within"], 6.0);
        assert_eq!(report["f_statistic"], 13.0);
        assert!((report["p_value"].as_f64().unwrap() - 0.006_591_796_875).abs() < 1e-10);
    }

    #[test]
    fn spearman_uses_average_ranks_for_ties() {
        let report = spearman(&[1.0, 2.0, 2.0, 4.0], &[10.0, 20.0, 30.0, 40.0]).unwrap();
        assert!((report["rho"].as_f64().unwrap() - 0.948_683_298_050_513_8).abs() < 1e-12);
        assert!(report["p_value_two_sided_approximation"].as_f64().unwrap() < 0.1);
    }

    #[test]
    fn fisher_correlation_interval_is_ordered_and_contains_r() {
        let report = pearson_interval(0.5, 30).unwrap();
        assert!(report["lower"].as_f64().unwrap() < 0.5);
        assert!(report["upper"].as_f64().unwrap() > 0.5);
        assert!(pearson_interval(1.0, 30).is_err());
    }

    #[test]
    fn bootstrap_intervals_are_deterministic_and_bound_the_estimate() {
        let sample = [1.0, 2.0, 3.0, 20.0];
        let mean = bootstrap_mean_interval(&sample, &Job::new()).unwrap();
        let again = bootstrap_mean_interval(&sample, &Job::new()).unwrap();
        assert_eq!(mean, again);
        assert!(mean["lower"].as_f64().unwrap() <= mean["estimate"].as_f64().unwrap());
        assert!(mean["upper"].as_f64().unwrap() >= mean["estimate"].as_f64().unwrap());
        let median = bootstrap_median_interval(&sample, &Job::new()).unwrap();
        assert!(median["lower"].as_f64().unwrap() <= 2.5);
        assert!(median["upper"].as_f64().unwrap() >= 2.5);
    }

    #[test]
    fn statistical_routes_reject_small_or_constant_samples() {
        assert!(mean_confidence_interval(&[1.0]).is_err());
        assert!(welch_t_test(&[1.0, 1.0], &[1.0, 1.0]).is_err());
        assert!(spearman(&[1.0, 1.0, 1.0], &[1.0, 2.0, 3.0]).is_err());
        assert!(pearson_interval(0.1, 3).is_err());
    }
}
