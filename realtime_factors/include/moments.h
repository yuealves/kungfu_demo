#pragma once
#include <cstdint>
#include <cmath>
#include <limits>

namespace rtf {

// Python demo 中 ε = float(np.finfo(np.float16).eps) = 0.0009765625
static constexpr double MOMENTS_EPS = 0.0009765625;

class Moments {
public:
    Moments() = default;

    void update(double x) {
        n_++;
        s1_ += x;
        double x2 = x * x;
        s2_ += x2;
        s3_ += x2 * x;
        s4_ += x2 * x2;
    }

    // 直接累加合并（等价于 Python 中 pl.col(c).sum()）
    static Moments merge(const Moments& a, const Moments& b) {
        Moments r;
        r.n_  = a.n_  + b.n_;
        r.s1_ = a.s1_ + b.s1_;
        r.s2_ = a.s2_ + b.s2_;
        r.s3_ = a.s3_ + b.s3_;
        r.s4_ = a.s4_ + b.s4_;
        return r;
    }

    int64_t count() const { return n_; }
    double s1() const { return s1_; }
    double s2() const { return s2_; }
    double s3() const { return s3_; }
    double s4() const { return s4_; }

    double mean() const {
        if (n_ == 0) return NAN;
        return s1_ / n_;
    }

    // 样本标准差: sqrt(max((s2 - s1²/n) / (n-1), 0))
    double std_sample() const {
        if (n_ < 2) return NAN;
        double var = (s2_ - s1_ * s1_ / n_) / (n_ - 1);
        return std::sqrt(std::max(var, 0.0));
    }

    // skew: population moments 计算
    double skew() const {
        if (n_ < 3) return NAN;
        double mu = s1_ / n_;
        double vp = s2_ / n_ - mu * mu;  // population variance
        if (vp <= MOMENTS_EPS * mu * mu) return NAN;
        double sp = std::sqrt(vp);
        double num = s3_ / n_ - 3.0 * mu * (s2_ / n_) + 2.0 * mu * mu * mu;
        return num / (sp * sp * sp);
    }

    // kurt: excess kurtosis (减3)
    double kurt() const {
        if (n_ < 4) return NAN;
        double mu = s1_ / n_;
        double vp = s2_ / n_ - mu * mu;
        if (vp <= MOMENTS_EPS * mu * mu) return NAN;
        double sp2 = vp;  // σ²
        double num = s4_ / n_ - 4.0 * mu * (s3_ / n_)
                   + 6.0 * mu * mu * (s2_ / n_) - 3.0 * mu * mu * mu * mu;
        return num / (sp2 * sp2) - 3.0;
    }

private:
    int64_t n_ = 0;
    double s1_ = 0, s2_ = 0, s3_ = 0, s4_ = 0;
};

} // namespace rtf
