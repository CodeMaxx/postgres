export POSTGRES_INSTALLDIR=/home/tarun/Desktop/dbProject/postgres/install
export POSTGRES_SRCDIR=/home/tarun/Desktop/dbProject/postgres/
./configure --prefix=${POSTGRES_INSTALLDIR} --enable-debug
export enable_debug=yes
make | tee make.out
make install | tee make_install.out
export LD_LIBRARY_PATH=${POSTGRES_INSTALLDIR}/lib:${LD_LIBRARY_PATH}
export PATH=${POSTGRES_INSTALLDIR}/bin:${PATH}

export PGDATA=${POSTGRES_INSTALLDIR}/data

initdb -D ${PGDATA}