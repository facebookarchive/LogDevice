## Best practices regarding header files and dependencies


### The problem

LogDevice is a C++ codebase of nontrivial size.  Because C++ is what it is, we need to take care to keep our build times and sizes tolerable (barely).

A big issue for build times is high-fan-in-high-fan-out headers.  For a header file `Foo.h`:
- Fan-in refers to how many compilation units (.cpp files) include the header file `Foo.h` (directly or indirectly).
- Fan-out refers to how many other header files `Foo.h` includes (again directly or indirectly).

High-fan-in headers themselves are a problem, as when those headers are modified, a lot of .cpp files need to be recompiled.  However, the combination of high fan-in and high fan-out means a change in any of the files *included from* `Foo.h` files causes us to rebuild all of the .cpp files that include `Foo.h`.  Also, because all of the .cpp files indirectly include a lot of code, build artifacts will be large due to all the debug info generated when compiling them (GCC seems to do a good job at maximising the amount of debug info emitted, when minimising would be preferred).

A general approach for limiting dependency explosion is to move as much code as possible to .cpp files, especially #include directives pulling in definitions of classes.


### Forward declarations

One tool for moving dependencies into .cpp files is forward declarations for types (classes, structs etc).  The key insight (with repercussions thankfully built into the language) is that declarations using types don't necessarily need to know the full definition of a type.  For example, this is fine:

  // fn.h
  class Foo;
  void fn(const Foo& foo);  // we don't know what is actually in `Foo`

When `fn` is declared, the compiler doesn't need to know what `Foo` looks like.  It only needs this when `Foo` is used, which is typically in the definition of `fn`, which lives in the associated .cpp file:

  // fn.cpp
  #include "Foo.h"  // pull in the definition of `Foo`
  void fn(const Foo& foo) {
    foo.use();
  }

Classes can contain members that are pointers and references to forward-declared types:

  // BigClass.h
  class Foo;
  class BigClass {
    Foo* foo_;
  };

Since pointers to all types are the same size [1], the compiler doesn't need to know the size or layout of `Foo` to determine the layout of `BigClass`.

A less obvious capability is to forward-declare return and parameter types (passed by value, not just pointers) in functions:

  // fn.h
  class Foo;
  class Bar;
  Foo fn(Bar bar);

The definition of `Foo` and `Bar` needs to be available at the callsites and implementation point of `fn` (both of which are often in .cpp files) but not in the header file declaring `fn`.

For a more complete set of examples, see http://stackoverflow.com/questions/553682/when-can-i-use-a-forward-declaration.  For examples of how we use forward declarations to reduce the amount of transitive dependencies in our codebase, see common/Worker.h and common/Processor.h.


### Forward declarations of smart-pointed types

Since smart pointers are pointers, it seems like we should be able to use forward decalartions with them, for example:

  // BigClass.h
  class Foo;
  class BigClass {
  public:
    ...
  private:
    Foo* foo1_;                  // this is fine
    std::unique_ptr<Foo> foo2_;  // we would like this to also be fine
  };

This can be made to work with a little care.  The biggest issue is that the compiler needs the definition of `Foo` to destroy a `unique_ptr<Foo>` (and also to default-construct the `unique_ptr` which is unfortunate).  In the above example, `BigClass` gets a default inlined constructor and destructor which require the definition of `Foo`, so anyone trying to create or destroy `BigClass` needs to separately pull in the definition of `Foo` in addition to `BigClass.h` (breaking encapsulation if `Foo` is a private member of `BigClass`).

A better solution is to de-inline the constructor/destructor and implement them in `BigClass.cpp`:

  // BigClass.h
  class Foo;
  class BigClass {
  public:
    BigClass();
    ~BigClass();
  private:
    Foo* foo1_;                  // this is fine
    std::unique_ptr<Foo> foo2_;  // also fine due to de-inlining
  };

  // BigClass.cpp
  #include "Foo.h"
  BigClass::BigClass();
  BigClass::~BigClass();

(De-inlining constructors and destructors is also a good idea in general as it helps reduce binary size.)


### Large-fwd.h headers

Sometimes a large header file `Large.h` defines several small types that are used inside and outside the class (examples are enums, parameter/result pack types etc).  It's wasteful to require users of these small types to include `Large.h` but it's also annoying to define small header files for each of the small types.  A compromise solution is to write a header file `Large-fwd.h` containing a subset of the declarations/definitions that users of `Large.h` are likely to find useful.

This pattern can also be useful with large headers containing templated code which cannot easily be offloaded into a .cpp file.  An example of this is the `<iosfwd>` header in the standard library.  It allows us to write header files that pass around `istream` instances, while only including the full `<istream>` and similar headers where the definition is required (in our .cpp files that actually manipulate the streams).


### PIMPL idiom

Another common approach for moving code into .cpp files is to limit implementation details of a class that get exposed to the external world.  Take for example the following class:

  // BigClass.h
  #include "Foo.h"
  #include "Bar.h"
  #include "Baz.h"
  class BigClass {
  public:
    ...
  private:
    Foo foo_;
    Bar bar_;
    Baz baz_;
  };

Users of `BigClass` don't care about its private members, yet as written they will definitions of their types when they pull in the above header.

One approach to reducing the depencency creep is to make each of the private members its own `unique_ptr`.  This introduces performance overhead in requiring dynamic memory allocation for each member, and decreases memory locality.

With the PIMPL ("private implementation") idiom, we combine several of the techniques already discussed to introduce a single forward-declared class that will be defined in the .cpp file and contain the private members:

  // BigClass.h
  class BigClassImpl;
  class BigClass {
  public:
    ...
  private:
    std::unique_ptr<BigClassImpl> impl_;
  };

We use this pattern in our codebase for several of our larger classes like `Worker` and `Processor`.

Another way of implementing this pattern is with subclassing (`BigClassImpl` subclasses `BigClass`).  Implementations of public `BigClass` methods can downcast `this` to `BigClassImpl` to access the private parts which gets rid of the `unique_ptr` indirection, however it requires `BigClass` to hide its constructors and expose a factory method that creates `BigClassImpl` under the hood, so that the downcast always succeeds.  We use this pattern for our classes in our public client interface, like `Client`.


[1] This is not guaranteed but it happens to be the case in our cozy x64 world.
