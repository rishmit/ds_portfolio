import streamlit as st

st.set_page_config(layout = "wide", page_title = "PRNG Analysis App")
st.markdown("""
            <h1 style = 'text-align: center; color: white;'>
            IMPLEMENTATION OF TAUSWORTHE, L’ECUYER AND MERSENNE TWISTER PSEUDO-RANDOM NUMBER GENERATORS
            </h1>""", unsafe_allow_html = True)
st.markdown("<div style = 'text-align: center; color: white;'>ISyE 6644: Simulation and Modeling for Engineering and Science</div>", unsafe_allow_html = True)
st.markdown("<div style = 'text-align: center; color: white;'>Georgia Institute of Technology, USA</div>", unsafe_allow_html = True)


st.subheader("ABSTRACT")

text_abstract = "Pseudo-random number generators play a fundamental role in a wide range of computational applica￾tions, including stochastic simulation and modeling techniques such as the Monte Carlo method, as well as in areas like computer gaming, online gambling, and cryptographic systems. This project focuses on the implementation of three widely studied PRNGs—Tausworthe, L’Ecuyer’s combined multiple recur￾sive generator (MRG32k3a), and the Mersenne Twister—within a Python programming environment. To evaluate the suitability of these generators, a number of statistical tests were conducted to assess whether their outputs exhibit characteristics of being independently and identically distributed (i.i.d.) with a uniform distribution over the interval (0,1). The tests performed include both visual diagnos￾tics (e.g., histograms, 2D and 3D scatter plots) and formal statistical procedures (e.g., Chi-square test, Kolmogorov-Smirnov test, runs tests, serial correlation analysis, and the Von Neumann ratio test). Ad￾ditionally, an application-based test was carried out using a Monte Carlo simulation to estimate the value of π, providing a practical benchmark for evaluating the performance of the generators."

st.markdown(f'<div style="text-align: justify;">{text_abstract}</div>', unsafe_allow_html = True)

st.subheader("1. BACKGROUND & DESCRIPTION")

text_background_1 = """
        <p>A Pseudo-Random Number Generator (PRNG) is an algorithm that produces a sequence of numbers
that appear to be random but are actually generated deterministically by a computer. While true
random numbers are said to be nondeterministic, since they are impossible to determine in advance,
pseudo-random numbers are not truly random because they are based on a predictable process and
hence the term ‘pseudo’.</p>

<p>A typical PRNG operates using a defined structure involving a finite set of internal states, denoted
by S, and a state transition function f : S → S. Alongside this, there is an output function g : S →
(0,1) that maps each internal state to a corresponding real number in the interval (0,1), which represents
the generated random number. The process begins with an initial state S0, commonly referred to as the
seed, which is usually supplied by the user [1].</p>

<p>The generator produces a sequence of states and corresponding outputs recursively as follows [1]:</p>
"""
st.markdown(f'<div style="text-align: justify;">{text_background_1}</div>', unsafe_allow_html = True)

st.latex(r"""
    \begin{equation}
    \begin{gathered}
        S_n = f(S_{n-1}), \hspace{0.2cm}\text{\textit{n} = 1, 2, 3, ...}\\
        U_n = g(S_n)
    \end{gathered}
\end{equation}
""")

text_background_2 = """<p>An important aspect of this mechanism is its deterministic nature: if the same seed is used to initialize the generator, the sequence of outputs will always be the same. Due to the finite nature of the state space, the generator is guaranteed to eventually revisit a previously encountered state, causing the sequence to repeat. The smallest positive integer \textit{p} for which the sequence returns to a former state after \textit{p} steps is referred to as the period of the generator. While a longer period is generally desirable, it alone does not guarantee high-quality of randomness. Other statistical properties must also be evaluated to ensure the generator produces outputs suitable for simulation and modeling applications.</p>
"""
st.markdown(f'<div style="text-align: justify;">{text_background_2}</div>', unsafe_allow_html = True)

st.markdown(
    """
    Some desirable properties of a generator are as follows:

    1.  **Uniformity:** The numbers generated appear to be distributed uniformly on (0, 1); 
    2.  **Independence:** The numbers generated show no correlation with each other; 
    3.  **Replication:** The numbers should be replicable (e.g., for debugging or comparison of different systems); 
    4.  **Cycle length:** It should take long before numbers start to repeat, in other words long period; 
    5.  **Speed:** The generator should be fast to produce numbers; 
    6.  **Memory usage:** The generator should not require a lot of storage. 
    """
)